package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cenkalti/backoff"
)

const (
	ExitCodeOK             int = 0
	ExitCodeError          int = 1
	ExitCodeFlagParseError     = 10 + iota
	ExitCodeAWSError

	DefaultBatchSize        int           = 1000
	ProgressRefreshInterval time.Duration = 100 * time.Millisecond
)

const helpText string = `Usage: s3rm [options]

Options:
  -bucket      The target S3 bucket name
  -dryrun      Run through object list without actually deleting anything
  -file        A file containing the object keys to be deleted
  -help        Print this message and exit
  -output      A file to write deleted object keys to
  -pool        Max worker pool size (default: 10)
  -prefix      List and delete all objects with this prefix
  -region      The AWS region of the target bucket
`

var (
	pool                *Pool
	jobStart            time.Time
	totalObjects        int64
	totalDeletedObjects int64

	// file descriptors
	outputFile *os.File

	// channels
	slowDown       chan int
	taskErrors     chan error
	deletedObjects chan []*s3.ObjectIdentifier

	// flags
	flagBucket string
	flagDryrun bool
	flagFile   string
	flagHelp   bool
	flagOutput string
	flagPool   int
	flagPrefix string
	flagRegion string
)

type DeleteTask struct {
	client  *s3.S3
	dryrun  bool
	Bucket  string
	Objects []*s3.ObjectIdentifier
}

func (t *DeleteTask) Execute() error {
	if t.dryrun {
		deletedObjects <- t.Objects
		return nil
	}

	operation := func() error {
		_, err := t.client.DeleteObjects(&s3.DeleteObjectsInput{
			Bucket: aws.String(t.Bucket),
			Delete: &s3.Delete{
				Objects: t.Objects,
				Quiet:   aws.Bool(true),
			},
		})

		// check for slow down error
		if err != nil {
			if reqerr, ok := err.(awserr.RequestFailure); ok {
				if reqerr.Code() == "SlowDown" {
					return err
				}
			}
			return &backoff.PermanentError{Err: err}
		}
		deletedObjects <- t.Objects
		return nil
	}
	return backoff.RetryNotify(operation, backoff.NewExponentialBackOff(), backoffNotify)
}

func backoffNotify(e error, t time.Duration) {
	slowDown <- 1
}

func printProgress() {
	var (
		prefix string
		detail string
	)
	if flagDryrun {
		prefix = "[dryrun] "
	}
	detail = fmt.Sprintf("%d workers", pool.Size)
	seconds := int64(time.Since(jobStart).Seconds())
	if totalDeletedObjects > 0 && seconds > 0 {
		detail = fmt.Sprintf("%s, %d obj/s", detail, totalDeletedObjects/seconds)
	}
	fmt.Printf("\r%sdelete: %d of %d objects (%s)", prefix, totalDeletedObjects, totalObjects, detail)
}

func main() {
	// initialize channels
	slowDown = make(chan int)
	taskErrors = make(chan error, 128)
	deletedObjects = make(chan []*s3.ObjectIdentifier, 128)

	flags := flag.NewFlagSet("flags", flag.ContinueOnError)
	flags.BoolVar(&flagHelp, "help", false, "")
	flags.StringVar(&flagBucket, "bucket", "", "")
	flags.BoolVar(&flagDryrun, "dryrun", false, "")
	flags.StringVar(&flagFile, "file", "", "")
	flags.StringVar(&flagOutput, "output", "", "")
	flags.IntVar(&flagPool, "pool", 10, "")
	flags.StringVar(&flagPrefix, "prefix", "", "")
	flags.StringVar(&flagRegion, "region", "us-east-1", "")

	// check flag values
	if err := flags.Parse(os.Args[1:]); err != nil {
		fmt.Println(err.Error())
		os.Exit(ExitCodeFlagParseError)
	}

	if flagHelp {
		fmt.Printf(helpText)
		os.Exit(ExitCodeOK)
	}

	if flagBucket == "" {
		fmt.Fprintln(os.Stderr, "Please provide a bucket name")
		os.Exit(ExitCodeFlagParseError)
	}

	var compl int
	batchSize := DefaultBatchSize

	// setup output file
	if flagOutput != "" {
		f, err := os.Create(flagOutput)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		outputFile = f
	}

	// create elastic worker pool
	pool = NewPool(flagPool)

	// make sure we don't go too fast
	go func() {
		for {
			<-slowDown
			if pool.Size > 1 {
				pool.Resize(pool.Size - 1)
			}
			time.Sleep(time.Second)
		}
	}()

	sess := session.Must(session.NewSession(
		&aws.Config{Region: &flagRegion},
	))
	svc := s3.New(sess)

	var (
		err     error
		scanner Scanner
	)

	if flagFile != "" {
		scanner, err = NewFileScanner(flagFile)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(ExitCodeError)
		}
	} else if flagPrefix != "" {
		scanner, err = NewBucketScanner(flagBucket, flagPrefix, svc)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(ExitCodeError)
		}
	} else {
		fmt.Fprintln(os.Stderr, "Please provide an s3 prefix or an objects file")
		os.Exit(ExitCodeFlagParseError)
	}

	go func() {
		for {
			select {
			case objects := <-deletedObjects:
				atomic.AddInt64(&totalDeletedObjects, int64(len(objects)))
				if flagOutput != "" {
					var output []string
					for _, obj := range objects {
						output = append(output, fmt.Sprintf("delete: %s", *obj.Key))
					}
					_, err := outputFile.WriteString(fmt.Sprintln(strings.Join(output, "\n")))
					if err != nil {
						fmt.Fprintln(os.Stderr, err)
						os.Exit(1)
					}
				}
			case err := <-pool.errors:
				fmt.Fprintln(os.Stderr, err)
			}
		}
	}()

	// track time for calculating delete rate
	jobStart = time.Now()

	// start progress bar
	go func() {
		for {
			printProgress()
			time.Sleep(ProgressRefreshInterval)
		}
	}()

	for scanner.Scan(batchSize) {
		totalObjects = totalObjects + int64(len(scanner.Objects()))
		pool.Exec(&DeleteTask{
			dryrun:  flagDryrun,
			client:  svc,
			Bucket:  flagBucket,
			Objects: scanner.Objects(),
		})
		compl = compl + batchSize
	}

	if scanner.Err() != nil {
		fmt.Fprintln(os.Stderr, scanner.Err())
		os.Exit(1)
	}

	pool.Close()
	pool.Wait()
	printProgress()
	fmt.Println("")
}
