# recordcache

## About
Recordcache provides abstractions around building and reading lsmtrees.

`RecordLogAppender` is used to write record logs.

`RecordLogDirectoryPoller` provides an iterator over record logs built by `RecordLogAppender`. It is typically used in conjunction with `RecordCache` to build lsmtrees. It persists the last position it processes so it can begin where it left off from previous runs.

`RecordCache` is an interface for performing lookups. There are two implementations.

`PersistentRecordCache` builds an lsmtree of key -> record log position. When performing lookups it uses the lsmtree as indirection into record logs to get the values.

`MemcachedMappingRecordCache` is a wrapper around `PersistentRecordCache` that uses memcached to eagerly cache new record log entries and any missed lookups.

`RecordLogStore` is a simple wrapper around a `Store`, `RecordLogDirectory`, and `RecordLogDirectoryPoller`.

`ReplicatingStoreBuilder` is a builder that creates a `RecordLogStore`. The store builds an lsmtree of key -> value and avoids the indirection of `PersistentRecordCache`. It deletes the underlying record logs after their contents have been written into the lsmtree.