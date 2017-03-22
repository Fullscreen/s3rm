s3rm
====

Delete tons of s3 objects efficiently.

Usage
=====
```shell
$ s3rm -h
Usage: s3rm [options]

Options:
  -h           Print this message and exit
  -bucket      The s3 bucket
  -dryrun      Print side-effects without actually deleting anything
  -file        A file containing the objects to be deleted
  -pool        Max worker pool size (default: 10)
  -region      The AWS region of the bucket
  -silent      Don't print delete objects

```

Output statistics update in real-time
```shell
$ s3rm -bucket mybucket -file objects_to_delete.txt -pool 30
Deleted 43000 of 202000 objects (30 workers / 6142 objects/s)
```

Planned Features
================

- AWS credentials profile support
