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
	exitCodeOK             int = 0
	exitCodeError          int = 1
	exitCodeFlagParseError     = 10 + iota
	exitCodeAWSError
)

const helpText string = `Usage: s3rm [options]

Options:
  -h           Print this message and exit
  -bucket      The s3 bucket
  -dryrun      Print side-effects without actually deleting anything
  -file        A file containing the objects to be deleted
  -pool        Max worker pool size (default: 10)
  -region      The AWS region of the bucket
  -silent      Don't print delete objects
`

var (
	pool                *Pool
	jobStart            time.Time
	silent              bool
	totalObjects        int64
	totalDeletedObjects int64

	// channels
	slowDown       chan int
	taskErrors     chan error
	deletedObjects chan int

	// flags
	flagHelp   bool
	flagBucket string
	flagDryrun bool
	flagFile   string
	flagPool   int
	flagPrefix string
	flagRegion string
	flagSilent bool
)

type DeleteTask struct {
	client  *s3.S3
	dryrun  bool
	Bucket  string
	Objects []*s3.ObjectIdentifier
}

func (t *DeleteTask) Execute() error {
	if t.dryrun {
		deletedObjects <- len(t.Objects)
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
		deletedObjects <- len(t.Objects)
		return nil
	}
	return backoff.RetryNotify(operation, backoff.NewExponentialBackOff(), backoffNotify)
}

func (t *DeleteTask) Done() {
	var (
		output []string
		prefix = "delete:"
	)
	if flagSilent == false {
		if t.dryrun {
			prefix = "[dryrun] delete:"
		}
		for _, obj := range t.Objects {
			output = append(output, fmt.Sprintf("%s %s", prefix, *obj.Key))
		}
		fmt.Println(strings.Join(output, "\n"))
	}
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
		detail = fmt.Sprintf("%s / %d obj/s", detail, totalDeletedObjects/seconds)
	}
	fmt.Printf("\r%sDeleted %d of %d objects (%s)", prefix, totalDeletedObjects, totalObjects, detail)
}

func main() {
	// initialize channels
	slowDown = make(chan int)
	taskErrors = make(chan error, 128)
	deletedObjects = make(chan int, 128)

	flags := flag.NewFlagSet("flags", flag.ContinueOnError)
	flags.BoolVar(&flagHelp, "h", false, "")
	flags.StringVar(&flagBucket, "bucket", "", "")
	flags.BoolVar(&flagDryrun, "dryrun", false, "")
	flags.StringVar(&flagFile, "file", "", "")
	flags.IntVar(&flagPool, "pool", 10, "")
	flags.StringVar(&flagPrefix, "prefix", "", "")
	flags.StringVar(&flagRegion, "region", "us-east-1", "")
	flags.BoolVar(&flagSilent, "silent", false, "")

	// check flag values
	if err := flags.Parse(os.Args[1:]); err != nil {
		fmt.Println(err.Error())
		os.Exit(exitCodeFlagParseError)
	}

	if flagHelp {
		fmt.Printf(helpText)
		os.Exit(exitCodeOK)
	}

	if flagBucket == "" {
		fmt.Fprintln(os.Stderr, "Please provide a bucket name")
		os.Exit(exitCodeFlagParseError)
	}

	var compl int
	batchSize := 1000

	// create elastic worker pool
	pool = NewPool(flagPool)

	// make sure we don't go too fast
	go func() {
		for {
			<-slowDown
			if pool.Size > 1 {
				pool.Resize(pool.Size - 1)
			}
			printProgress()
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
			os.Exit(exitCodeError)
		}
	} else if flagPrefix != "" {
		scanner, err = NewBucketScanner(flagBucket, flagPrefix, svc)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(exitCodeError)
		}
	} else {
		fmt.Fprintln(os.Stderr, "Please provide an s3 prefix or an objects file")
		os.Exit(exitCodeFlagParseError)
	}

	go func() {
		for {
			select {
			case count := <-deletedObjects:
				atomic.AddInt64(&totalDeletedObjects, int64(count))
				printProgress()
			case err := <-pool.errors:
				fmt.Fprintln(os.Stderr, err)
			}
		}
	}()

	// track time for calculating delete rate
	jobStart = time.Now()

	for scanner.Scan(batchSize) {
		totalObjects = totalObjects + int64(len(scanner.Objects()))
		pool.Exec(&DeleteTask{
			dryrun:  flagDryrun,
			client:  svc,
			Bucket:  flagBucket,
			Objects: scanner.Objects(),
		})
		printProgress()
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
