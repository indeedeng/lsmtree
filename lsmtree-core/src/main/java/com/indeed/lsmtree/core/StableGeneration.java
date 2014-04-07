package com.indeed.lsmtree.core;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.indeed.util.compress.CompressionCodec;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.serialization.LongSerializer;
import com.indeed.util.serialization.Serializer;
import com.indeed.util.core.shell.PosixFileOperations;
import com.indeed.lsmtree.recordlog.BlockCompressedRecordFile;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * @author jplaisance
 */
public final class StableGeneration {

    private static final Logger log = Logger.getLogger(StableGeneration.class);

    public static <K,V> Generation<K,V> open(BloomFilter.MemoryManager memoryManager, File file, Comparator<K> comparator, Serializer<K> keySerializer, Serializer<V> valueSerializer, StorageType storageType, CompressionCodec codec, final boolean mlockBTree)
            throws IOException {
        if (storageType == StorageType.BLOCK_COMPRESSED && codec == null) throw new IllegalArgumentException("codec must be set if block compressed");
        if (storageType == StorageType.INLINE) {
            return new InlineStableGeneration<K, V>(memoryManager, file, comparator, keySerializer, valueSerializer, mlockBTree);
        } else if (storageType == StorageType.BLOCK_COMPRESSED) {
            return new BlockCompressedStableGeneration<K, V>(memoryManager, file, comparator, keySerializer, valueSerializer, codec, mlockBTree);
        } else {
            throw new IllegalArgumentException(storageType+" is not a valid storage type");
        }
    }

    public static class InlineStableGeneration<K,V> implements Generation<K, V> {

        private final BloomFilter.Reader bloomFilter;

        private final ImmutableBTreeIndex.Reader<K,V> reader;

        private final File file;

        public InlineStableGeneration(BloomFilter.MemoryManager memoryManager, File file, Comparator<K> comparator,  Serializer<K> keySerializer, Serializer<V> valueSerializer, final boolean mlockBTree) throws IOException {
            this.file = file;
            reader = new ImmutableBTreeIndex.Reader(file, comparator, keySerializer, valueSerializer, mlockBTree);
            final File bloomFilterFile = new File(file, "bloomfilter.bin");
            if (bloomFilterFile.exists()) {
                bloomFilter = new BloomFilter.Reader(memoryManager, bloomFilterFile, keySerializer);
            } else {
                bloomFilter = null;
            }
        }

        @Override
        public Entry<K, V> get(final K key) {
            if (bloomFilter == null || bloomFilter.contains(key)) {
                return reader.get(key);
            }
            return null;
        }

        @Override
        public @Nullable Boolean isDeleted(final K key) {
            final Entry<K, V> entry = get(key);
            return entry == null ? null : (entry.isDeleted() ? Boolean.TRUE : Boolean.FALSE);
        }

        @Override
        public Generation<K, V> head(K end, boolean inclusive) {
            return reader.head(end, inclusive);
        }

        @Override
        public Generation<K, V> tail(K start, boolean inclusive) {
            return reader.tail(start, inclusive);
        }

        @Override
        public Generation<K, V> slice(K start, boolean startInclusive, K end, boolean endInclusive) {
            return reader.slice(start, startInclusive, end, endInclusive);
        }

        @Override
        public Generation<K, V> reverse() {
            return reader.reverse();
        }

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return reader.iterator();
        }

        @Override
        public Iterator<Entry<K, V>> iterator(K start, boolean startInclusive) {
            return reader.iterator(start, startInclusive);
        }

        @Override
        public Iterator<Entry<K, V>> reverseIterator() {
            return reader.reverseIterator();
        }

        @Override
        public Iterator<Entry<K, V>> reverseIterator(K start, boolean startInclusive) {
            return reader.reverseIterator(start, startInclusive);
        }

        @Override
        public Comparator<K> getComparator() {
            return reader.getComparator();
        }

        @Override
        public long size() {
            return reader.size();
        }

        @Override
        public long sizeInBytes() {
            return reader.sizeInBytes()+(bloomFilter == null ? 0 : bloomFilter.sizeInBytes());
        }

        @Override
        public boolean hasDeletions() {
            return reader.hasDeletions();
        }

        @Override
        public void close() throws IOException {
            Closeables2.closeQuietly(reader, log);
            if (bloomFilter != null) Closeables2.closeQuietly(bloomFilter, log);
        }

        @Override
        public void delete() throws IOException {
            log.info("deleting " + getPath());
            PosixFileOperations.rmrf(getPath());
        }

        @Override
        public File getPath() {
            return file;
        }

        @Override
        public void checkpoint(final File checkpointPath) throws IOException {
            PosixFileOperations.cplr(file, checkpointPath);
        }
    }

    public static class BlockCompressedStableGeneration<K,V> implements Generation<K, V> {

        private final BloomFilter.Reader bloomFilter;

        private final ImmutableBTreeIndex.Reader<K,Long> reader;

        private final BlockCompressedRecordFile<V> recordFile;

        private final File file;

        private final long sizeInBytes;

        private final SharedReference<Closeable> stuffToClose;

        public BlockCompressedStableGeneration(
                BloomFilter.MemoryManager memoryManager, File file, Comparator<K> comparator, Serializer<K> keySerializer, Serializer<V> valueSerializer, final CompressionCodec codec, final boolean mlockBTree
        ) throws IOException {
            this.file = file;
            reader = new ImmutableBTreeIndex.Reader(file, comparator, keySerializer, new LongSerializer(), mlockBTree);
            final File valuesFile = new File(file, "values.bin");
            recordFile =
                    new BlockCompressedRecordFile.Builder(valuesFile, valueSerializer, codec).setMlockFiles(mlockBTree).build();
            final File bloomFilterFile = new File(file, "bloomfilter.bin");
            if (bloomFilterFile.exists()) {
                bloomFilter = new BloomFilter.Reader(memoryManager, bloomFilterFile, keySerializer);
            } else {
                bloomFilter = null;
            }
            sizeInBytes = reader.sizeInBytes()+valuesFile.length()+(bloomFilter == null ? 0 : bloomFilter.sizeInBytes());
            stuffToClose = SharedReference.create((Closeable)new Closeable() {
                public void close() throws IOException {
                    Closeables2.closeQuietly(reader, log);
                    if (bloomFilter != null) Closeables2.closeQuietly(bloomFilter, log);
                    Closeables2.closeQuietly(recordFile, log);
                }
            });
        }

        @Override
        public Entry<K, V> get(final K key) {
            if (bloomFilter == null || bloomFilter.contains(key)) {
                final Entry<K, Long> result = reader.get(key);
                if (result == null) return null;
                return lookupValue(result);
            }
            return null;
        }

        @Override
        public @Nullable Boolean isDeleted(final K key) {
            if (bloomFilter == null || bloomFilter.contains(key)) {
                final Entry<K, Long> result = reader.get(key);
                return result == null ? null : (result.isDeleted() ? Boolean.TRUE : Boolean.FALSE);
            }
            return null;
        }

        private Entry<K, V> lookupValue(Entry<K, Long> result) {
            if (result.isDeleted()) return Entry.createDeleted(result.getKey());
            try {
                return Entry.create(result.getKey(), recordFile.get(result.getValue()));
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }

        @Override
        public Generation<K, V> head(K end, boolean inclusive) {
            return new FilteredGeneration<K, V>(this, stuffToClose.copy(), null, false, end, inclusive);
        }

        @Override
        public Generation<K, V> tail(K start, boolean inclusive) {
            return new FilteredGeneration<K, V>(this, stuffToClose.copy(), start, inclusive, null, false);
        }

        @Override
        public Generation<K, V> slice(K start, boolean startInclusive, K end, boolean endInclusive) {
            return new FilteredGeneration<K, V>(this, stuffToClose.copy(), start, startInclusive, end, endInclusive);
        }

        @Override
        public Generation<K, V> reverse() {
            return new ReverseGeneration<K, V>(this, stuffToClose.copy());
        }

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return iterator(reader.iterator());
        }

        @Override
        public Iterator<Entry<K, V>> iterator(K start, boolean startInclusive) {
            return iterator(reader.iterator(start, startInclusive));
        }
        
        private Iterator<Entry<K, V>> iterator(final Iterator<Entry<K, Long>> it) {
            return new Iterator<Entry<K, V>>() {

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Entry<K, V> next() {
                    final Entry<K, Long> next = it.next();
                    return lookupValue(next);
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public Iterator<Entry<K, V>> reverseIterator() {
            return iterator(reader.reverseIterator());
        }

        @Override
        public Iterator<Entry<K, V>> reverseIterator(K start, boolean startInclusive) {
            return iterator(reader.reverseIterator(start, startInclusive));
        }

        @Override
        public long size() {
            return reader.size();
        }

        @Override
        public long sizeInBytes() {
            return sizeInBytes;
        }

        @Override
        public boolean hasDeletions() {
            return reader.hasDeletions();
        }

        @Override
        public Comparator<K> getComparator() {
            return reader.getComparator();
        }

        @Override
        public void close() throws IOException {
            stuffToClose.close();
        }

        @Override
        public void delete() throws IOException {
            log.info("deleting " + getPath());
            PosixFileOperations.rmrf(getPath());
        }

        @Override
        public File getPath() {
            return file;
        }

        @Override
        public void checkpoint(final File checkpointPath) throws IOException {
            PosixFileOperations.cplr(file, checkpointPath);
        }
    }

    public static class Writer {

        public static <K,V> void write(BloomFilter.MemoryManager memoryManager, File path, final List<Generation<K,V>> generations, Serializer<K> keySerializer, final Serializer<V> valueSerializer, Comparator<K> keyComparator, StorageType storageType, CompressionCodec codec, boolean hasDeletions)
                throws IOException {
            write(memoryManager, path, generations, keySerializer, valueSerializer, keyComparator, storageType, codec, hasDeletions, true);
        }

        public static <K,V> void write(BloomFilter.MemoryManager memoryManager, File path, final List<Generation<K,V>> generations, Serializer<K> keySerializer, final Serializer<V> valueSerializer, Comparator<K> keyComparator, StorageType storageType, CompressionCodec codec, boolean hasDeletions, boolean useBloomFilter)
                throws IOException {
            ImmutableBTreeIndex.Reader reader = null;
            if (storageType == StorageType.INLINE) {
                final Iterator<Generation.Entry<K,V>> iterator = mergeIterators(generations, keyComparator);
                final long start = System.nanoTime();
                ImmutableBTreeIndex.Writer.write(path, iterator, keySerializer, valueSerializer, 65536, hasDeletions);
                log.info("write b tree time: "+(System.nanoTime()-start)/1000d+" us");
                if (useBloomFilter) {
                    reader = new ImmutableBTreeIndex.Reader(path, keySerializer, valueSerializer, false);
                }
            } else if (storageType == StorageType.BLOCK_COMPRESSED) {
                path.mkdirs();
                final File valuesFile = new File(path, "values.bin");
                final Iterator<Generation.Entry<K,V>> iterator = mergeIterators(generations, keyComparator);
                final BlockCompressedRecordFile.Writer<V> writer = BlockCompressedRecordFile.Writer.open(valuesFile, valueSerializer, codec, 16384, 10, 6);
                final Iterator<Generation.Entry<K,Long>> keyAddressIterator = new Iterator<Generation.Entry<K,Long>>() {

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Generation.Entry<K,Long> next() {
                        final Generation.Entry<K, V> next = iterator.next();
                        try {
                            if (next.isDeleted()) {
                                return Generation.Entry.createDeleted(next.getKey());
                            } else {
                                return Generation.Entry.create(next.getKey(), writer.append(next.getValue()));
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
                final LongSerializer longSerializer = new LongSerializer();
                final long start = System.nanoTime();
                ImmutableBTreeIndex.Writer.write(path, keyAddressIterator, keySerializer, longSerializer, 65536, hasDeletions);
                writer.close();
                log.info("write b tree time: "+(System.nanoTime()-start)/1000d+" us");
                if (useBloomFilter) {
                    reader = new ImmutableBTreeIndex.Reader(path, keySerializer, longSerializer, false);
                }
            }
            if (reader != null) {
                final long start = System.nanoTime();
                if (reader.size() > 0 && reader.size() <= Integer.MAX_VALUE) {
                    final File file = new File(path, "bloomfilter.bin");
                    try {
                        BloomFilter.Writer.write(memoryManager, file, reader.iterator(), keySerializer, reader.size());
                    } catch (BloomFilter.NotEnoughMemoryException e) {
                        log.warn("not enough memory to write bloomfilter", e);
                        file.delete();
                    }
                }
                reader.close();
                log.info("write bloom filter time: "+(System.nanoTime()-start)/1000d+" us");
            }
        }
        
        private static <K,V> Iterator<Generation.Entry<K, V>> mergeIterators(final List<Generation<K,V>> generations, final Comparator<K> keyComparator) throws IOException {
            return new MergingIterator<K,V>(Lists.transform(generations, new Function<Generation<K, V>, Iterator<Generation.Entry<K, V>>>() {
                @Override
                public Iterator<Generation.Entry<K, V>> apply(Generation<K, V> input) {
                    return input.iterator();
                }
            }), keyComparator);
        }
    }
}
