# recordlog

## About
Recordlog is a library for reading and writing data to record logs. Many records are stored together in a `RecordFile`.

## API Documentation
`RecordFile` is a generic interface for a collection of byte arrays to be addressed by 64 bit pointers.

`RecordFile.Reader` provides iteration over a RecordFile and can start seeked to a position.
`RecordFile.Reader` has the following methods:

`boolean next()`
Seeks to next entry, returns true if one exists

`long getPosition()`
Returns position of current entry

`E get()`
Gets value of current entry

`RecordFile.Writer` provides one method:

`long append(E entry)`
Appends the entry provided to the file and return the address used for accessing it later


There are three implementations of `RecordFile`.

`BasicRecordFile` stores entries sequentially as serialized entry byte length, CRC32 checksum, entry bytes.

`BlockCompressedRecordFile` is a `RecordFile` implementation that compresses the byte[] entries in blocks of a given size.  The compression algorithm is pluggable.  The block size is configurable.  There are a maximum of 2^recordIndexBits records per block.  Each block is padded to a multiple of 2^padBits bytes.  Within each block there is a header containing an int for the number of entries and a list of varint encoded ints for the offsets into the block for each entry.  The address scheme uses the low recordIndexBits for the index in the record offset list of the record.  The rest of the bits are used for the address.  Since the low padBits bits are always 0 they are not stored.  There is an additional class called BlockCache which enables blocks to be cached across readers using a weak valued concurrent computing hash map.

`RecordLogDirectory` is a `RecordFile` implementation which stores lots of BlockCompressedRecordFiles in a directory in sequentially numbered segment files.  The writer provides the additional method roll(), which synchronizes the current file and starts a new one for future appends.  RecordLogDirectory uses the top 28 bits of the address for the segment file number, which limits each individual segment file to 4 GB (they can be slightly larger but the last addressable block is at address 2^32).  RecordLogDirectory maintains a fixed size cache of segments addressed by segment number and containing block caches.  The block caches are reference counted to make closing the file handles possible when they are evicted from the cache.

## CompressedBlockRecordFile file format
```
record log: [block]*[MAX_INT (int)][metadata][metadata length (int)][total size]

block: [block size (int)][block checksum (int)][num records (int)][entry length (vint)]*[entry]*[padding]
                                               |----------------- compressed ------------------|
```

Metadata is arbitrary bytes that can be written as part of the record log. When using RecordLogAppender (see lsmtree-recordcache) this is unused and metadata is written to a separate file.
Checksum is an Adler-32 checksum of the compressed portion of the block.
Each entry length is a variable-length integer, representing the byte length of the corresponding entry.
Padding is used to align the start of block positions such that the address can be represented with fewer bits later.

## Address scheme

Addresses are represented by 64-bit values.

BlockCompressedRecordFile address scheme (not written as a RecordLogDirectory):
```
[block address: 54 bits][record index: 10 bits]
```

RecordLogDirectory address scheme:
```
[segment number: 28 bits][block address: 26 bits][record index: 10 bits]
```

In both of these schemes the block address is implicitly padBits (default 6) longer. For RecordLogDirectory record logs, this means the block address is 32 bits, but the last 6 bits must be zeroes. Thus the last block address can start at 0xffffffc0 and the max record file size is approximately 4 GB.

