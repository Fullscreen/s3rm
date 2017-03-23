s3rm
====

Delete tons of s3 objects efficiently.

Usage
=====
```shell
$ s3rm -h
Usage: s3rm [options]

Options:
  -bucket      The target S3 bucket name
  -dryrun      Run through object list without actually deleting anything
  -file        A file containing the object keys to be deleted
  -help        Print this message and exit
  -output      A file to write deleted object keys to
  -pool        Max worker pool size (default: 10)
  -prefix      List and delete all objects with this prefix
  -region      The AWS region of the target bucket
```

Output statistics update in real-time
```shell
$ s3rm -bucket mybucket -file objects_to_delete.txt -pool 30
delete: 43000 of 202000 objects (30 workers, 6142 obj/s)
```

Planned Features
================

- AWS credentials profile support
