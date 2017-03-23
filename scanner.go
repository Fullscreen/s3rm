package main

import (
	"bufio"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Scanner interface {
	Err() error
	Scan(count int) bool
	Objects() []*s3.ObjectIdentifier
}

type FileScanner struct {
	buf     []*s3.ObjectIdentifier
	scanner *bufio.Scanner
}

type BucketScanner struct {
	Bucket string
	Prefix string
	client *s3.S3
	err    error
	buf    []*s3.ObjectIdentifier
}

func (s *FileScanner) Scan(count int) bool {
	s.buf = nil
	for i := 0; i < count; i++ {
		if s.scanner.Scan() {
			obj := &s3.ObjectIdentifier{Key: aws.String(s.scanner.Text())}
			s.buf = append(s.buf, obj)
		} else {
			// return if this is the first read and the scanner is empty
			if len(s.buf) == 0 {
				return false
			}
		}
	}
	return true
}

func (s *FileScanner) Err() error {
	return nil
}

func (s *FileScanner) Objects() []*s3.ObjectIdentifier {
	return s.buf
}

func NewFileScanner(file string) (*FileScanner, error) {
	fd, err := os.Open(file)
	if err != nil {
		return &FileScanner{}, err
	}
	list := &FileScanner{
		scanner: bufio.NewScanner(fd),
	}
	return list, nil
}

func (s *BucketScanner) Scan(count int) bool {
	var marker *string
	if len(s.buf) > 0 {
		marker = s.buf[len(s.buf)-1].Key
	}

	s.buf = nil
	params := &s3.ListObjectsInput{
		Bucket:  aws.String(s.Bucket),
		Marker:  marker,
		MaxKeys: aws.Int64(int64(count)),
		Prefix:  aws.String(s.Prefix),
	}
	resp, err := s.client.ListObjects(params)
	if err != nil {
		s.err = err
		return false
	}

	if len(resp.Contents) < 1 {
		return false
	}
	for _, object := range resp.Contents {
		s.buf = append(s.buf, &s3.ObjectIdentifier{Key: object.Key})
	}
	return true
}

func (s *BucketScanner) Err() error {
	return s.err
}

func (s *BucketScanner) Objects() []*s3.ObjectIdentifier {
	return s.buf
}

func NewBucketScanner(bucket string, prefix string, client *s3.S3) (*BucketScanner, error) {
	return &BucketScanner{Bucket: bucket, Prefix: prefix, client: client}, nil
}
