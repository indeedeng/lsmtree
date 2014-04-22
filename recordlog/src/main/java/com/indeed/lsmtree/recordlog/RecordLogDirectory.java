package com.indeed.lsmtree.recordlog;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.indeed.util.compress.CompressionCodec;
import com.indeed.util.compress.Decompressor;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.serialization.Serializer;
import fj.data.Option;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author jplaisance
 */
public final class RecordLogDirectory<E> implements RecordFile<E> {

    private static final Logger log = Logger.getLogger(RecordLogDirectory.class);

    public static final int DEFAULT_FILE_INDEX_BITS = 28;

    public static final int DEFAULT_RECORD_INDEX_BITS = 10;

    public static final int DEFAULT_PAD_BITS = 6;

    public static final int DEFAULT_BLOCK_SIZE = 16384;

    public static class Writer<E> implements RecordFile.Writer<E> {

        private final int blockSize;

        private final int recordIndexBits;

        private final int padBits;

        private final int segmentShift;

        private final File path;

        private final Serializer<E> serializer;
        private final CompressionCodec codec;

        private final File tmpPath;

        private File currentWriterPath;

        private final long rollFrequency;

        private long lastRollTime;

        private RecordFile.Writer currentWriter;

        private int currentSegmentNum;

        public static <E> Writer<E> create(File file, Serializer<E> serializer, CompressionCodec codec, final long rollFrequency) throws IOException {
            return new Writer(file, serializer, codec, rollFrequency, DEFAULT_BLOCK_SIZE, DEFAULT_FILE_INDEX_BITS, DEFAULT_RECORD_INDEX_BITS, DEFAULT_PAD_BITS, -1);
        }

        public static <E> Writer<E> create(
                File file, Serializer<E> serializer, CompressionCodec codec, final long rollFrequency, int blockSize, int fileIndexBits, int recordIndexBits, int padBits
        ) throws IOException {
            return new Writer(file, serializer, codec, rollFrequency, blockSize, fileIndexBits, recordIndexBits, padBits, -1);
        }

        public static <E> Writer<E> create(File file, Serializer<E> serializer, CompressionCodec codec, final long rollFrequency, int maxSegment) throws IOException {
            return new Writer(file, serializer, codec, rollFrequency, DEFAULT_BLOCK_SIZE, DEFAULT_FILE_INDEX_BITS, DEFAULT_RECORD_INDEX_BITS, DEFAULT_PAD_BITS, maxSegment);
        }

        public static <E> Writer<E> create(
                File file,
                Serializer<E> serializer,
                CompressionCodec codec,
                final long rollFrequency,
                int blockSize,
                int fileIndexBits,
                int recordIndexBits,
                int padBits,
                int maxSegment
        ) throws IOException {
            return new Writer(file, serializer, codec, rollFrequency, blockSize, fileIndexBits, recordIndexBits, padBits, maxSegment);
        }

        private Writer(
                File file, Serializer<E> serializer, CompressionCodec codec, final long rollFrequency, int blockSize, int fileIndexBits, int recordIndexBits, int padBits, int maxSegment
        ) throws IOException {
            this.path = file;
            this.serializer = serializer;
            this.codec = codec;
            this.blockSize = blockSize;
            this.recordIndexBits = recordIndexBits;
            this.padBits = padBits;
            segmentShift = 64-fileIndexBits;
            this.tmpPath = new File(file, "tmp");
            this.rollFrequency = rollFrequency;
            tmpPath.mkdirs();
            if (maxSegment < 0) {
                currentSegmentNum = getMaxSegmentNum(path);
                if (currentSegmentNum == -1 || verifySegmentIntegrity(path, currentSegmentNum)) currentSegmentNum++;
            } else {
                currentSegmentNum = maxSegment+1;
            }
            log.info("current segment num: "+currentSegmentNum);
            currentWriter = createWriter(currentSegmentNum);
            lastRollTime = System.currentTimeMillis();
        }

        private RecordFile.Writer createWriter(int segmentNum) throws IOException {
            currentWriterPath = new File(tmpPath, String.valueOf(segmentNum)+".rec");
            return BlockCompressedRecordFile.Writer.open(currentWriterPath, serializer, codec, blockSize, recordIndexBits, padBits);
        }

        @Override
        public long append(final E entry) throws IOException {
            if (System.currentTimeMillis()-lastRollTime > rollFrequency) {
                roll();
            }
            final long writerAddress = currentWriter.append(entry);
            if (writerAddress >= 1L<< segmentShift) throw new IOException("current writer has exceeded maximum size");
            return (((long)currentSegmentNum)<< segmentShift)+writerAddress;
        }

        public void roll() throws IOException {
            currentWriter.sync();
            currentWriter.close();
            final File segmentFile = getSegmentPath(path, currentSegmentNum, true);
            currentWriterPath.renameTo(segmentFile);
            currentWriter = createWriter(++currentSegmentNum);
            lastRollTime = System.currentTimeMillis();
        }

        public boolean verifySegmentIntegrity(File file, int segmentNum) {
            BlockCompressedRecordFile<E> recordFile = null;
            try {
                recordFile =
                        new BlockCompressedRecordFile.Builder(getSegmentPath(file, segmentNum, false), serializer, codec)
                                .setBlockSize(blockSize)
                                .setRecordIndexBits(recordIndexBits)
                                .setPadBits(padBits)
                                .build();
            } catch (IOException e) {
                return false;
            } finally {
                if (recordFile != null) try {
                    recordFile.close();
                } catch (IOException e) {
                    //ignore
                }
            }
            return true;
        }

        @Override
        public void close() throws IOException {
            try {
                roll();
            } finally {
                currentWriter.close();
            }
        }

        @Override
        public void sync() throws IOException {
            currentWriter.sync();
        }
    }

    public static final class Builder<E> {

        private File dir;

        private Serializer<E> serializer;

        private int maxCachedFiles = 1024;

        private CompressionCodec codec;

        private int blockSize = DEFAULT_BLOCK_SIZE;

        private int fileIndexBits = DEFAULT_FILE_INDEX_BITS;

        private int recordIndexBits = DEFAULT_RECORD_INDEX_BITS;

        private int padBits = DEFAULT_PAD_BITS;

        private boolean mlockFiles;

        public Builder(final File dir, final Serializer<E> serializer, final CompressionCodec codec) {
            this.dir = dir;
            this.serializer = serializer;
            this.codec = codec;
        }

        public Builder<E> setDir(final File dir) {
            this.dir = dir;
            return this;
        }

        public Builder<E> setSerializer(final Serializer<E> serializer) {
            this.serializer = serializer;
            return this;
        }

        public Builder<E> setMaxCachedFiles(final int maxCachedFiles) {
            this.maxCachedFiles = maxCachedFiles;
            return this;
        }

        public Builder<E> setCodec(final CompressionCodec codec) {
            this.codec = codec;
            return this;
        }

        public Builder<E> setBlockSize(final int blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        public Builder<E> setFileIndexBits(final int fileIndexBits) {
            this.fileIndexBits = fileIndexBits;
            return this;
        }

        public Builder<E> setRecordIndexBits(final int recordIndexBits) {
            this.recordIndexBits = recordIndexBits;
            return this;
        }

        public Builder<E> setPadBits(final int padBits) {
            this.padBits = padBits;
            return this;
        }

        public Builder<E> setMlockFiles(boolean mlockFiles) {
            this.mlockFiles = mlockFiles;
            return this;
        }

        public File getDir() {
            return dir;
        }

        public Serializer<E> getSerializer() {
            return serializer;
        }

        public int getMaxCachedFiles() {
            return maxCachedFiles;
        }

        public CompressionCodec getCodec() {
            return codec;
        }

        public int getBlockSize() {
            return blockSize;
        }

        public int getFileIndexBits() {
            return fileIndexBits;
        }

        public int getRecordIndexBits() {
            return recordIndexBits;
        }

        public int getPadBits() {
            return padBits;
        }

        public boolean isMlockFiles() {
            return mlockFiles;
        }

        public RecordLogDirectory<E> build() {
            return new RecordLogDirectory(dir, serializer, maxCachedFiles, codec, blockSize, fileIndexBits, recordIndexBits, padBits, mlockFiles);
        }
    }

    private final FileCache fileCache;
    private final int segmentShift;
    private final long segmentMask;
    private final File dir;
    private final Serializer<E> serializer;
    private final int maxCachedFiles;
    private final CompressionCodec codec;
    private final int blockSize;
    private final int fileIndexBits;
    private final int recordIndexBits;
    private final int padBits;

    public RecordLogDirectory(
            File dir,
            Serializer<E> serializer,
            int maxCachedFiles,
            CompressionCodec codec,
            int blockSize,
            int fileIndexBits,
            int recordIndexBits,
            int padBits
    ) {
        this(dir, serializer, maxCachedFiles, codec, blockSize, fileIndexBits, recordIndexBits, padBits, false);
    }

    public RecordLogDirectory(
            File dir,
            Serializer<E> serializer,
            int maxCachedFiles,
            CompressionCodec codec,
            int blockSize,
            int fileIndexBits,
            int recordIndexBits,
            int padBits,
            boolean mlockFiles
    ) {
        this.dir = dir;
        this.serializer = serializer;
        this.maxCachedFiles = maxCachedFiles;
        this.codec = codec;
        this.blockSize = blockSize;
        this.fileIndexBits = fileIndexBits;
        this.recordIndexBits = recordIndexBits;
        this.padBits = padBits;
        segmentShift = 64-fileIndexBits;
        segmentMask = (1L<< segmentShift)-1;
        fileCache = new FileCache(mlockFiles);
    }

    @Override
    public E get(long address) throws IOException {
        final int segmentNum = (int)(address>>> segmentShift);
        final Option<SharedReference<BlockCompressedRecordFile<E>>> option = fileCache.get(segmentNum);
        if (option.isNone()) {
            throw new IOException("address is invalid: "+address);
        }
        final SharedReference<BlockCompressedRecordFile<E>> reference = option.some();
        final E ret;
        try {
            ret = reference.get().get(address & segmentMask);
        } finally {
            Closeables2.closeQuietly(reference, log);
        }
        return ret;
    }

    @Override
    public RecordFile.Reader<E> reader() throws IOException {
        return new Reader();
    }

    @Override
    public RecordFile.Reader<E> reader(long address) throws IOException {
        return new Reader(address);
    }

    public Option<RecordFile.Reader<E>> getFileReader(final long segmentNum) throws IOException {
        final Option<SharedReference<BlockCompressedRecordFile<E>>> option = fileCache.get((int) segmentNum);
        if (option.isNone()) return Option.none();
        final SharedReference<BlockCompressedRecordFile<E>> reference = option.some();
        final RecordFile.Reader<E> segmentReader;
        try {
            segmentReader = reference.get().reader();
        } finally {
            Closeables2.closeQuietly(reference, log);
        }
        return Option.<RecordFile.Reader<E>>some(new RecordFile.Reader<E>() {

            final long segmentShift = 64-fileIndexBits;
            final long maxSegmentPosition = (1L << segmentShift)-1;

            @Override
            public boolean next() throws IOException {
                return segmentReader.next();
            }

            @Override
            public long getPosition() {
                final long segmentPosition = segmentReader.getPosition();
                if (segmentPosition > maxSegmentPosition) {
                    throw new IllegalStateException("position in segment file"+segmentNum+" is too high to be addressable in record log directory with "+fileIndexBits+" file index bits");
                }
                return (segmentNum<<segmentShift)+segmentPosition;
            }

            @Override
            public E get() {
                return segmentReader.get();
            }

            @Override
            public void close() throws IOException {
                Closeables2.closeQuietly(segmentReader, log);
            }
        });
    }

    public void garbageCollect(final long address) {
        int segmentNum = getSegmentNum(address)-1;
        final List<File> filesToDelete = Lists.newArrayList();
        while (segmentNum >= 0) {
            final File f = getSegmentPath(dir, segmentNum);
            segmentNum--;
            if (f.exists()) {
                filesToDelete.add(f);
            } else {
                break;
            }
        }
        for (int i = filesToDelete.size()-1; i >= 0; i--) {
            filesToDelete.get(i).delete();
        }
    }

    @Override
    public void close() throws IOException {
        fileCache.close();
    }

    public int getSegmentNum(long address) {
        return (int) (address >>> (64-fileIndexBits));
    }

    public long getAddress(long segNum) {
        return segNum << (64-fileIndexBits);
    }

    public int getMaxSegmentNum() throws IOException {
        return getMaxSegmentNum(dir);
    }

    public long getSegmentTimestamp(int segmentNum) throws IOException {
        return getSegmentPath(dir, segmentNum, false).lastModified();
    }

    private class Reader implements RecordFile.Reader<E> {

        private RecordFile.Reader<E> currentReader = null;

        private int currentSegmentNum = 0;

        private boolean done = false;

        private Reader() throws IOException {}

        public Reader(long address) throws IOException {
            currentSegmentNum = (int)(address>>> segmentShift);
            final Option<SharedReference<BlockCompressedRecordFile<E>>> recordFile = fileCache.get(currentSegmentNum);
            if (recordFile.isNone()) {
                throw new IOException("address is invalid: "+address);
            }
            final SharedReference<BlockCompressedRecordFile<E>> reference = recordFile.some();
            try {
                currentReader = reference.get().reader(address & segmentMask);
            } finally {
                Closeables2.closeQuietly(reference, log);
            }
        }

        @Override
        public boolean next() throws IOException {
            if (done) return false;
            if (currentReader == null) {
                if (!getSegmentReader(currentSegmentNum)) {
                    done = true;
                    return false;
                }
            }
            while (!currentReader.next()) {
                if (!getSegmentReader(currentSegmentNum+1)) {
                    done = true;
                    return false;
                }
                currentSegmentNum++;
            }
            return true;
        }

        @Override
        public long getPosition() {
            return (((long)currentSegmentNum)<< segmentShift)+currentReader.getPosition();
        }

        @Override
        public E get() {
            return currentReader.get();
        }

        private boolean getSegmentReader(int segmentNum) throws IOException {
            Closeables2.closeQuietly(currentReader, log);
            final Option<SharedReference<BlockCompressedRecordFile<E>>> option = fileCache.get(segmentNum);
            for (SharedReference<BlockCompressedRecordFile<E>> reference : option) {
                try {
                    currentReader = reference.get().reader();
                } finally {
                    Closeables2.closeQuietly(reference, log);
                }
                return true;
            }
            return false;
        }

        @Override
        public void close() {
            Closeables2.closeQuietly(currentReader, log);
        }
    }

    public static File getSegmentPath(File file, int segmentNum, boolean mkDirs) {
        File segmentDir = file;
        for (int divisor = 1000000; divisor > 1; divisor/=1000) {
            segmentDir = new File(segmentDir, String.format("%03d", (segmentNum/divisor)%1000));
        }
        if (mkDirs) segmentDir.mkdirs();
        return new File(segmentDir, String.format("%09d", segmentNum)+".rec");
    }

    public static int getMaxSegmentNum(File path) throws IOException {
        int maxSegmentNum = -1;
        int maxDir = -1;
        if (path.exists()) {
            for (File f : path.listFiles()) {
                final String name = f.getName();
                if (name.matches("\\d+") && f.isDirectory()) {
                    final int dirNum = Integer.parseInt(name);
                    if (dirNum > maxDir) maxDir = dirNum;
                } else if (name.matches("\\d+\\.rec")) {
                    final int segmentNum = Integer.parseInt(name.substring(0, name.length()-4));
                    if (segmentNum > maxSegmentNum) maxSegmentNum = segmentNum;
                }
            }
            if (maxSegmentNum >= 0) return maxSegmentNum;
            if (maxDir >= 0) return getMaxSegmentNum(new File(path, String.format("%03d", maxDir)));
        }
        return -1;
    }

    public static int getMinSegmentNum(File path) throws IOException {
        int minSegmentNum = Integer.MAX_VALUE;
        int minDir = Integer.MAX_VALUE;
        if (path.exists()) {
            for (File f : path.listFiles()) {
                final String name = f.getName();
                if (name.matches("\\d+") && f.isDirectory()) {
                    final int dirNum = Integer.parseInt(name);
                    if (dirNum < minDir) minDir = dirNum;
                } else if (name.matches("\\d+\\.rec")) {
                    final int segmentNum = Integer.parseInt(name.substring(0, name.length()-4));
                    if (segmentNum < minSegmentNum) minSegmentNum = segmentNum;
                }
            }
            if (minSegmentNum < Integer.MAX_VALUE) return minSegmentNum;
            if (minDir < Integer.MAX_VALUE) return getMinSegmentNum(new File(path, String.format("%03d", minDir)));
        }
        return -1;
    }

    public static File getSegmentPath(File root, int segment) {
        return getSegmentPath(root, segment, false);
    }

    private class FileCache implements Closeable {
        private final LoadingCache<Integer, Option<SharedReference<BlockCompressedRecordFile<E>>>> readerCache;

        private final BlockingQueue<Decompressor> decompressorPool;

        private final boolean mlockFiles;

        public FileCache(final boolean mlockFiles) {
            this.mlockFiles = mlockFiles;
            decompressorPool = new LinkedBlockingQueue<Decompressor>();
            readerCache = CacheBuilder.newBuilder().maximumSize(maxCachedFiles)
                    .removalListener(new RemovalListener<Integer, Option<SharedReference<BlockCompressedRecordFile<E>>>>() {
                        @Override
                        public void onRemoval(RemovalNotification<Integer, Option<SharedReference<BlockCompressedRecordFile<E>>>> notification) {
                            final Integer segmentNum = notification.getKey();
                            final Option<SharedReference<BlockCompressedRecordFile<E>>> referenceOption = notification.getValue();
                            for (SharedReference<BlockCompressedRecordFile<E>> reference : referenceOption) {
                                try {
                                    reference.close();
                                } catch (IOException e) {
                                    log.error("error on block cleanup", e);
                                }
                            }
                        }
                    })
                    .build(open);
        }

        private final CacheLoader<Integer, Option<SharedReference<BlockCompressedRecordFile<E>>>> open = new CacheLoader<Integer, Option<SharedReference<BlockCompressedRecordFile<E>>>>() {
            @Override
            public Option<SharedReference<BlockCompressedRecordFile<E>>> load(final Integer segmentNum) {
                try {
                    final File segmentFile = getSegmentPath(dir, segmentNum, false);
                    if (!segmentFile.exists()) {
                        return Option.none();
                    }
                    final long start = System.nanoTime();
                    final BlockCompressedRecordFile<E> recordFile =
                            new BlockCompressedRecordFile.Builder(segmentFile, serializer, codec)
                                    .setDecompressorPool(decompressorPool)
                                    .setBlockSize(blockSize)
                                    .setRecordIndexBits(recordIndexBits)
                                    .setPadBits(padBits)
                                    .setMlockFiles(mlockFiles)
                                    .build();
                    log.debug("segment open time: "+(System.nanoTime()-start)/1000d+" us");
                    return Option.some(SharedReference.create(recordFile));
                } catch (IOException e) {
                    log.error("error opening file with segment number "+segmentNum, e);
                    return Option.none();
                }
            }
        };

        public Option<SharedReference<BlockCompressedRecordFile<E>>> get(final Integer key) {
            final Option<SharedReference<BlockCompressedRecordFile<E>>> option = readerCache.getUnchecked(key);
            if (option.isNone()) {
                readerCache.invalidate(key);
                return option;
            } else {
                final SharedReference<BlockCompressedRecordFile<E>> copy = option.some().tryCopy();
                if (copy == null) {
                    return get(key);
                }
                return Option.some(copy);
            }
        }

        @Override
        public void close() throws IOException {
            readerCache.invalidateAll();
        }
    }
}
