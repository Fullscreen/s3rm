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
	if silent == false {
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
	if totalDeletedObjects > 0 {
		rate := totalDeletedObjects / int64(time.Since(jobStart).Seconds())
		fmt.Printf("\rDeleted %d of %d objects (%d workers / %d obj/s)", totalDeletedObjects, totalObjects, pool.Size, rate)
	} else {
		fmt.Printf("\rDeleted %d of %d objects (%d workers)", totalDeletedObjects, totalObjects, pool.Size)
	}
}

func main() {
	// initialize channels
	slowDown = make(chan int)
	taskErrors = make(chan error, 128)
	deletedObjects = make(chan int, 128)

	flags := flag.NewFlagSet("flags", flag.ContinueOnError)
	help := flags.Bool("h", false, "")
	bucket := flags.String("bucket", "", "")
	dryrun := flags.Bool("dryrun", false, "")
	file := flags.String("file", "", "")
	poolSize := flags.Int("pool", 10, "")
	prefix := flags.String("prefix", "", "")
	region := flags.String("region", "us-east-1", "")
	quiet := flags.Bool("silent", false, "")

	// check flag values
	if err := flags.Parse(os.Args[1:]); err != nil {
		fmt.Println(err.Error())
		os.Exit(exitCodeFlagParseError)
	}

	silent = *quiet

	if *help {
		fmt.Printf(helpText)
		os.Exit(exitCodeOK)
	}

	if *bucket == "" {
		fmt.Fprintln(os.Stderr, "Please provide a bucket name")
		os.Exit(exitCodeFlagParseError)
	}

	var compl int
	batchSize := 1000

	// create elastic worker pool
	pool = NewPool(*poolSize)

	// make sure we don't go too fast
	go func() {
		for {
			<-slowDown
			if pool.Size > 1 {
				pool.Resize(pool.Size - 1)
			}
			time.Sleep(time.Second * 5)
		}
	}()

	sess := session.Must(session.NewSession(
		&aws.Config{Region: region},
	))
	svc := s3.New(sess)

	var (
		err     error
		scanner Scanner
	)

	if *file != "" {
		scanner, err = NewFileScanner(*file)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(exitCodeError)
		}
	} else if *prefix != "" {
		scanner, err = NewBucketScanner(*bucket, *prefix, svc)
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
		printProgress()
		pool.Exec(&DeleteTask{
			dryrun:  *dryrun,
			client:  svc,
			Bucket:  *bucket,
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
}
