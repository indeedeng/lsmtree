# Indeed LSM Tree

## About

Indeed LSM Tree is a fast key/value store that is efficient for high-volume random access reads and writes.

Indeed uses it for many applications including serving job data for 100s of millions of job searches each day.

Indeed LSM Tree is composed of three parts:

1. [lsmtree-core](https://github.com/indeedeng/lsmtree/tree/master/lsmtree-core) is an implementation of a log-structured merge-tree.
2. [recordlog](https://github.com/indeedeng/lsmtree/tree/master/recordlog) is a library for writing data to append-only record logs optimized for replication.
3. [recordcache](https://github.com/indeedeng/lsmtree/tree/master/recordcache) provides abstractions around writing record logs, building LSM trees, and performing LSM tree lookups.

A typical use case is requiring a key/value store replicated on multiple servers. Instead of replicating the store itself, we store writes in record logs which are slaved to each server. Each server then builds its own LSM tree from the record logs.

## Benchmarks

10,000,000 operations using 8 byte keys and 96 byte values

Random writes:

| Software        | Time           |
| ------------- |:-------------:|
|Indeed LSM Tree|272 seconds|
|Google LevelDB|375 seconds|
|Kyoto Cabinet B-Tree|375 seconds|

Random Reads:

| Software        | Time           |
| ------------- |:-------------:|
|Indeed LSM Tree|46 seconds|
|Google LevelDB|80 seconds|
|Kyoto Cabinet B-Tree|183 seconds|

Same benchmark using cgroups to limit page cache to 512 MB

Random Reads:

| Software        | Time           |
| ------------- |:-------------:|
|Indeed LSM Tree|454 seconds|
|Google LevelDB|464 seconds|
|Kyoto Cabinet B-Tree|50 hours|
