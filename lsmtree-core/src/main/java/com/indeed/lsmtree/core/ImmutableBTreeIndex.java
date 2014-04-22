package com.indeed.lsmtree.core;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.io.LittleEndianDataOutputStream;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.core.shell.PosixFileOperations;
import com.indeed.util.io.BufferedFileDataOutputStream;
import com.indeed.util.mmap.Memory;
import com.indeed.util.mmap.MemoryDataInput;
import com.indeed.util.mmap.MMapBuffer;
import com.indeed.util.serialization.LongSerializer;
import com.indeed.util.serialization.Serializer;
import it.unimi.dsi.fastutil.chars.CharArrayList;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.Iterator;

/**
 * @author jplaisance
 */
public final class ImmutableBTreeIndex {

    private static final Logger log = Logger.getLogger(ImmutableBTreeIndex.class);

    public static final class Writer {

        //no reason to instantiate this ever
        private Writer() {}

        public static <K, V> void write(
                File file,
                Iterator<Generation.Entry<K,V>> iterator,
                Serializer<K> keySerializer,
                Serializer<V> valueSerializer,
                final int blocksize,
                boolean keepDeletions
        ) throws IOException {
            if (blocksize > 65536) throw new IllegalArgumentException("block size must be less than 65536");
            file.mkdirs();
            final BufferedFileDataOutputStream fileOut = new BufferedFileDataOutputStream(new File(file, "index.bin"));
            final CountingOutputStream out = new CountingOutputStream(fileOut);
            //tempFile is deleted in writeIndex
            final File tempFile = File.createTempFile("tmp", ".bin");
            final WriteLevelResult result = writeLevel(out, tempFile, iterator, keySerializer, valueSerializer, blocksize, keepDeletions);
            final int tmpCount = result.tmpCount;
            final long size = result.size;

            final long valueLevelLength = out.getCount();
            final Header header = writeIndex(out, tempFile, tmpCount, keySerializer, blocksize);
            header.valueLevelLength = valueLevelLength;
            header.size = size;
            header.hasDeletions = keepDeletions;
            new HeaderSerializer().write(header, new LittleEndianDataOutputStream(out));
            fileOut.sync();
            out.close();
        }

        private static Header writeIndex(
                final CountingOutputStream counter, File tempFile, int tmpCount, Serializer keySerializer, int blocksize
        ) throws IOException {
            if (tmpCount == 0) {
                tempFile.delete();
                final Header header = new Header();
                header.indexLevels = 0;
                header.rootLevelStartAddress = 0;
                header.fileLength = Header.length();
                return header;
            }
            TempFileIterator tmpIterator = new TempFileIterator(tempFile, tmpCount, keySerializer);
            int indexLevels = 0;
            final LongSerializer longSerializer = new LongSerializer();
            while (true) {
                final long levelStart = counter.getCount();
                indexLevels++;
                final File nextTempFile = File.createTempFile("tmp", ".bin");
                final WriteLevelResult result = writeLevel(counter, nextTempFile, tmpIterator, keySerializer, longSerializer, blocksize, false);
                tmpCount = result.tmpCount;
                tempFile.delete();
                if (tmpCount <= 1) {
                    nextTempFile.delete();
                    final Header header = new Header();
                    header.indexLevels = indexLevels;
                    header.rootLevelStartAddress = levelStart;
                    header.fileLength = counter.getCount()+Header.length();
                    return header;
                } else {
                    tempFile = nextTempFile;
                    tmpIterator = new TempFileIterator(tempFile, tmpCount, keySerializer);
                }
            }
        }

        private static <K,V> WriteLevelResult writeLevel(
                final CountingOutputStream counter,
                final File tempFile,
                final Iterator<Generation.Entry<K,V>> iterator,
                final Serializer<K> keySerializer,
                final Serializer<V> valueSerializer,
                final int blocksize,
                final boolean keepDeletions
        ) throws IOException {
            Generation.Entry<K,V> next;
            if (!iterator.hasNext()) {
                return new WriteLevelResult(0, 0);
            }
            next = iterator.next();
            final LittleEndianDataOutputStream tmpOut = new LittleEndianDataOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile), 131072));
            final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            final LittleEndianDataOutputStream bufferDataOutput = new LittleEndianDataOutputStream(buffer);
            final ByteArrayOutputStream currentBlock = new ByteArrayOutputStream(blocksize);
            final CharArrayList keyOffsets = new CharArrayList();
            int tmpCount = 0;
            boolean done = false;
            final LittleEndianDataOutputStream out = new LittleEndianDataOutputStream(counter);
            long count = 0;
            outer: while (!done) {

                currentBlock.reset();
                keyOffsets.clear();
                if (!keepDeletions) {
                    while (next.isDeleted()) {
                        if (!iterator.hasNext()) break outer;
                        next = iterator.next();
                    }
                }
                keySerializer.write(next.getKey(), tmpOut);
                tmpOut.writeLong(counter.getCount());
                tmpCount++;
                while (true) {
                    buffer.reset();
                    final boolean skipDeleted = updateBuffer(next, keySerializer, valueSerializer, keepDeletions, bufferDataOutput);
                    if (4+2*keyOffsets.size()+2+currentBlock.size()+buffer.size() > blocksize) {
                        if (currentBlock.size() == 0) {
                            throw new IllegalArgumentException("key value pair is greater than block size");
                        }
                        break;
                    }
                    if (!skipDeleted) {
                        keyOffsets.add((char)currentBlock.size());
                        buffer.writeTo(currentBlock);
                        count++;
                    }
                    if (!iterator.hasNext()) {
                        done = true;
                        break;
                    }
                    next = iterator.next();
                }
                if (keyOffsets.size() > 0) {
                    final long start = counter.getCount();
                    out.writeInt(keyOffsets.size());
                    for (int i = 0; i < keyOffsets.size(); i++) {
                        out.writeChar(keyOffsets.getChar(i));
                    }
                    currentBlock.writeTo(out);
                    if (counter.getCount()-start > blocksize) {
                        log.error("too big");
                    }
                }
            }
            tmpOut.close();
            return new WriteLevelResult(tmpCount, count);
        }

        private static final class WriteLevelResult {
            final int tmpCount;
            final long size;

            private WriteLevelResult(final int tmpCount, final long size) {
                this.tmpCount = tmpCount;
                this.size = size;
            }
        }

        private static<K,V> boolean updateBuffer(
                final Generation.Entry<K,V> entry, Serializer<K> keySerializer, Serializer<V> valueSerializer, final boolean keepDeletions, final DataOutput bufferDataOutput
        ) throws IOException {
            final boolean skipDeleted;
            if (keepDeletions) {
                skipDeleted = false;
                keySerializer.write(entry.getKey(), bufferDataOutput);
                if (entry.isDeleted()) {
                    bufferDataOutput.writeByte(1);
                } else {
                    bufferDataOutput.writeByte(0);
                    valueSerializer.write(entry.getValue(), bufferDataOutput);
                }
            } else {
                if (entry.isDeleted()) {
                    skipDeleted = true;
                } else {
                    skipDeleted = false;
                    keySerializer.write(entry.getKey(), bufferDataOutput);
                    valueSerializer.write(entry.getValue(), bufferDataOutput);
                }
            }
            return skipDeleted;
        }
    }

    private static final class TempFileIterator<K> implements Iterator<Generation.Entry<K, Long>> {


        private final LittleEndianDataInputStream in;

        private final int tmpCount;

        private final Serializer<K> keySerializer;
        private final LongSerializer longSerializer = new LongSerializer();

        private int i = 0;

        public TempFileIterator(
                File tempFile,
                int tmpCount,
                Serializer<K> keySerializer
        ) throws FileNotFoundException {
            this.tmpCount = tmpCount;
            this.keySerializer = keySerializer;
            in = new LittleEndianDataInputStream(new BufferedInputStream(new FileInputStream(tempFile), 131072));
        }

        @Override
        public boolean hasNext() {
            if (i < tmpCount) return true;
            Closeables2.closeQuietly(in, log);
            return false;
        }

        @Override
        public Generation.Entry<K, Long> next() {
            i++;
            try {
                final K key = keySerializer.read(in);
                final Long value = longSerializer.read(in);
                return Generation.Entry.create(key, value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public static final class Reader<K, V> implements Generation<K,V>, Closeable {

        private final MMapBuffer buffer;
        
        private final Level<K, V> rootLevel;

        private final long rootLevelStartAddress;

        private final boolean hasDeletions;

        private final long sizeInBytes;

        private final long size;

        private final File indexFile;
        
        private final Comparator<K> comparator;

        private final SharedReference<Closeable> stuffToClose;

        public Reader(File file, Serializer<K> keySerializer, Serializer<V> valueSerializer, final boolean mlockFiles) throws IOException {
            this(file, new ComparableComparator(), keySerializer, valueSerializer, mlockFiles);
        }

        public Reader(File file, Comparator<K> comparator, Serializer<K> keySerializer, Serializer<V> valueSerializer, final boolean mlockFiles) throws IOException {
            this.comparator = comparator;
            indexFile = new File(file, "index.bin");
            sizeInBytes = indexFile.length();
            buffer = new MMapBuffer(indexFile, FileChannel.MapMode.READ_ONLY, ByteOrder.LITTLE_ENDIAN);
            try {
                stuffToClose = SharedReference.create((Closeable)buffer);
                final MemoryDataInput in = new MemoryDataInput(buffer.memory());
                if (sizeInBytes < Header.length()) {
                    throw new IOException("file is less than header length bytes");
                }
                final byte[] headerBytes = new byte[Header.length()];
                in.seek(sizeInBytes - Header.length());
                in.readFully(headerBytes);
                final LittleEndianDataInputStream headerStream = new LittleEndianDataInputStream(new ByteArrayInputStream(headerBytes));
                final Header header = new HeaderSerializer().read(headerStream);
                hasDeletions = header.hasDeletions;
                size = header.size;
                if (header.fileLength != sizeInBytes) {
                    log.error(header.fileLength);
                    throw new IOException("file length written to last 8 bytes of file does not match file length, file is inconsistent");
                }
                rootLevel = Level.build(buffer.memory(), keySerializer, valueSerializer, comparator, header.hasDeletions, header.indexLevels);
                rootLevelStartAddress = header.rootLevelStartAddress;
                if (mlockFiles) buffer.mlock(0, buffer.memory().length());
            } catch (Throwable t) {
                Closeables2.closeQuietly(buffer, log);
                Throwables.propagateIfInstanceOf(t, IOException.class);
                throw Throwables.propagate(t);
            }
        }
        
        private Block<K,V> rootBlock() {
            return new Block<K, V>(null, 0, rootLevel, rootLevel.getBlock(rootLevelStartAddress));
        }

        @Nullable
        public Entry<K, V> get(K key) {
            try {
                final Block<K, V> valueBlock = rootBlock().getValueBlock(key);
                if (valueBlock == null) return null;
                return valueBlock.get(key);
            } catch (InternalError e) {
                throw new RuntimeException("file "+indexFile.getAbsolutePath()+" length is currently less than MMapBuffer length, it has been modified after open. this is a huge problem.", e);
            }
        }

        @Override
        public @Nullable Boolean isDeleted(final K key) {
            final Entry<K,V> entry = get(key);
            return entry == null ? null : (entry.isDeleted() ? Boolean.TRUE : Boolean.FALSE);
        }

        @Nullable public Entry<K, V> lower(K key) throws IOException {
            return neighbor(key, lower);
        }

        @Nullable public Entry<K, V> floor(K key) throws IOException {
            return neighbor(key, floor);
        }

        @Nullable public Entry<K, V> ceil(K key) throws IOException {
            return neighbor(key, ceil);
        }

        @Nullable public Entry<K, V> higher(K key) throws IOException {
            return neighbor(key, higher);
        }
        
        @Nullable private Entry<K, V> neighbor(K key, NeighborModifier modifier) throws IOException {
            try {
                final Block<K, V> valueBlock = rootBlock().getValueBlock(key);
                if (valueBlock == null) return null;
                return valueBlock.neighbor(key, modifier);
            } catch (InternalError e) {
                throw new IOException("file "+indexFile.getAbsolutePath()+" length is currently less than MMapBuffer length, it has been modified after open. this is a huge problem.", e);
            }
        }

        public Entry<K, V> first() throws IOException {
            try {
                final Block valueBlock = rootBlock().getFirstValueBlock();
                return valueBlock.dataBlock.getEntry(0);
            } catch (InternalError e) {
                throw new IOException("file "+indexFile.getAbsolutePath()+" length is currently less than MMapBuffer length, it has been modified after open. this is a huge problem.", e);
            }
        }

        public Entry<K, V> last() throws IOException {
            try {
                final Block valueBlock = rootBlock().getLastValueBlock();
                return valueBlock.dataBlock.getEntry(valueBlock.length()-1);
            } catch (InternalError e) {
                throw new IOException("file "+indexFile.getAbsolutePath()+" length is currently less than MMapBuffer length, it has been modified after open. this is a huge problem.", e);
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
            return iterator(null, false);
        }

        @Override
        public Iterator<Entry<K, V>> iterator(final @Nullable K start, final boolean startInclusive) {
            return new AbstractIterator<Entry<K, V>>() {
                
                Block<K,V> current = null;
                int currentIndex;

                @Override
                protected Entry<K, V> computeNext() {
                    if (current == null) {
                        final Block<K,V> rootBlock = rootBlock();
                        if (rootBlock == null) {
                            return endOfData();
                        }
                        if (start != null) {
                            current = rootBlock.getValueBlock(start);
                            if (current != null) {
                                final int insertionPoint = current.search(start);
                                if (insertionPoint >= 0) {
                                    currentIndex = startInclusive ? insertionPoint : insertionPoint + 1;
                                } else {
                                    currentIndex = ~insertionPoint;
                                }
                            }
                        }
                        if (current == null) {
                            current = rootBlock.getFirstValueBlock();
                            currentIndex = 0;
                        }
                    }
                    if (currentIndex >= current.length()) {
                        current = current.nextBlock();
                        if (current == null) {
                            return endOfData();
                        }
                        currentIndex = 0;
                    }
                    final Entry<K,V> ret = current.getEntry(currentIndex);
                    currentIndex++;
                    return ret;
                }
            };
        }

        @Override
        public Iterator<Entry<K, V>> reverseIterator() {
            return reverseIterator(null, false);
        }

        @Override
        public Iterator<Entry<K, V>> reverseIterator(final @Nullable K start, final boolean startInclusive) {
            return new AbstractIterator<Entry<K, V>>() {

                Block current = null;
                int currentIndex;

                @Override
                protected Entry<K, V> computeNext() {
                    if (current == null) {
                        final Block<K,V> rootBlock = rootBlock();
                        if (rootBlock == null) {
                            return endOfData();
                        }
                        if (start == null) {
                            current = rootBlock.getLastValueBlock();
                            currentIndex = current.length()-1;
                        } else {
                            current = rootBlock.getValueBlock(start);
                            if (current == null) {
                                return endOfData();
                            }
                            final int insertionPoint = current.search(start);
                            if (insertionPoint >= 0) {
                                currentIndex = startInclusive ? insertionPoint : insertionPoint - 1;
                            } else {
                                currentIndex = (~insertionPoint)-1;
                            }
                        }
                    }
                    if (currentIndex < 0) {
                        current = current.previousBlock();
                        if (current == null) {
                            return endOfData();
                        }
                        currentIndex = current.length()-1;
                    }
                    final Entry<K,V> ret = current.getEntry(currentIndex);
                    currentIndex--;
                    return ret;
                }
            };
        }

        @Override
        public Comparator<K> getComparator() {
            return comparator;
        }

        @Override
        public File getPath() {
            return indexFile;
        }

        @Override
        public void checkpoint(File checkpointPath) throws IOException {
            PosixFileOperations.cplr(indexFile, checkpointPath);
        }

        @Override
        public void delete() throws IOException {
            indexFile.delete();
        }

        public long sizeInBytes() {
            return sizeInBytes;
        }

        public long size() {
            return size;
        }

        @Override
        public void close() throws IOException {
            stuffToClose.close();
        }

        public boolean hasDeletions() {
            return hasDeletions;
        }

        private static final class Block<K,V> {

            final Block<K,V> parent;
            final int parentPosition;
            final Level<K,V> level;
            final Level<K,V>.DataBlock dataBlock;


            Block(@Nullable Block<K, V> parent, int parentPosition, Level<K, V> level, Level.DataBlock dataBlock) {
                this.parent = parent;
                this.parentPosition = parentPosition;
                this.level = level;
                this.dataBlock = dataBlock;
            }

            boolean isValueLevel() {
                return level.isValueLevel();
            }

            @Nullable Block<K,V> getChildBlock(int index) {
                final Level<K, V> nextLevel = Preconditions.checkNotNull(level.nextLevel);
                if (index >= dataBlock.length()) {
                    if (index == dataBlock.length()) {
                        final Block<K,V> nextBlock = nextBlock();
                        return nextBlock == null ? null : nextBlock.getChildBlock(0);
                    } else {
                        throw new RuntimeException();
                    }
                } if (index < 0) {
                    if (index == -1) {
                        final Block<K,V> previousBlock = previousBlock();
                        return previousBlock == null ? null : previousBlock.getChildBlock(previousBlock.dataBlock.length-1);
                    } else {
                        throw new RuntimeException();
                    }
                }
                final Long address = (Long) dataBlock.getEntry(index).getValue();
                return new Block(this, index, nextLevel, nextLevel.getBlock(address));
            }

            int length() {
                return dataBlock.length();
            }

            @Nullable Block<K,V> nextBlock() {
                return parent == null ? null : parent.getChildBlock(parentPosition+1);
            }

            @Nullable Block<K,V> previousBlock() {
                return parent == null ? null : parent.getChildBlock(parentPosition-1);
            }

            @Nullable Block<K,V> getContainingBlock(K key) {
                final Level<K, V> nextLevel = Preconditions.checkNotNull(level.nextLevel);
                final int floorIndex = neighborIndex(key, floor);
                final SearchResult<K, Long> searchResult = getSearchResult(floorIndex);
                return searchResult.match(new SearchResult.Matcher<K, Long, Block<K,V>>() {

                    Block<K, V> found(final Entry<K, Long> floor) {
                        return new Block(Block.this, floorIndex, nextLevel, nextLevel.getBlock(floor.getValue()));
                    }

                    @Nullable Block<K, V> low() {
                        return null;
                    }
                });
            }

            @Nullable Block<K,V> getValueBlock(K key) {
                if (isValueLevel()) return this;
                final Block<K, V> containingBlock = getContainingBlock(key);
                if (containingBlock == null) return null;
                return containingBlock.getValueBlock(key);
            }

            Block<K,V> getFirstValueBlock() {
                if (isValueLevel()) return this;
                final Block<K, V> childBlock = Preconditions.checkNotNull(getChildBlock(0));
                return childBlock.getFirstValueBlock();
            }

            Block<K,V> getLastValueBlock() {
                if (isValueLevel()) return this;
                final Block<K, V> childBlock = Preconditions.checkNotNull(getChildBlock(length() - 1));
                return childBlock.getLastValueBlock();
            }

            @Nullable Entry<K,V> get(K key) {
                if (!level.isValueLevel()) throw new RuntimeException();
                return (Entry<K,V>)dataBlock.get(key);
            }
            
            int neighborIndex(K key, NeighborModifier modifier) {
                final int insertionPoint = dataBlock.search(key);
                return insertionPoint >= 0 ? insertionPoint + modifier.addFound : (~insertionPoint) + modifier.addNotFound;
            }
            
            @Nullable <A> Entry<K,A> neighbor(final K key, final NeighborModifier modifier) {
                final SearchResult<K, A> lowerEntry = getSearchResult(neighborIndex(key, modifier));
                return lowerEntry.match(new SearchResult.Matcher<K, A, Entry<K, A>>() {

                    Entry<K, A> found(final Entry<K, A> entry) {
                        return entry;
                    }

                    @Nullable Entry<K, A> low() {
                        final Block<K, V> previousBlock = previousBlock();
                        return previousBlock == null ? null : previousBlock.<A>neighbor(key, modifier);
                    }

                    @Nullable Entry<K, A> high() {
                        final Block<K, V> nextBlock = nextBlock();
                        return nextBlock == null ? null : nextBlock.<A>neighbor(key, modifier);
                    }
                });
            }

            <A> SearchResult<K,A> getSearchResult(final int neighborIndex) {
                if (neighborIndex < 0) return Low.low();
                if (neighborIndex >= dataBlock.length()) return High.high();
                return new Found(dataBlock.getEntry(neighborIndex));
            }
            
            public K getKey(int index) {
                if (!level.isValueLevel()) throw new RuntimeException();
                return dataBlock.getKey(index);
            }
            
            public Entry<K,V> getEntry(int index) {
                if (!level.isValueLevel()) throw new RuntimeException();
                return (Entry<K,V>)dataBlock.getEntry(index);
            }
            
            int search(K key) {
                return dataBlock.search(key);
            }
        }

        private static interface SearchResult<K,V> {
            
            public <Z> Z match(Matcher<K,V,Z> m);
            
            static abstract class Matcher<K,V,Z> {
                Z found(Entry<K,V> entry) {return otherwise();}
                Z low() {return otherwise();}
                Z high() {return otherwise();}
                Z otherwise() {throw new UnsupportedOperationException();}
            }
        }
        
        private static final class Found<K,V> implements SearchResult<K,V> {
            final Entry<K,V> entry;

            private Found(final Entry<K, V> entry) {
                this.entry = entry;
            }

            public <Z> Z match(final Matcher<K,V,Z> m) {
                return m.found(entry);
            }
        }
        
        private static final class Low<K,V> implements SearchResult<K,V> {

            static <K,V> Low<K,V> low() {return low;}

            static final Low low = new Low();

            public <Z> Z match(final Matcher<K, V, Z> m) {
                return m.low();
            }
        }
        
        private static final class High<K,V> implements SearchResult<K,V> {
            
            static <K,V> High<K,V> high() {return high;}
            
            static final High high = new High();

            public <Z> Z match(final Matcher<K, V, Z> m) {
                return m.high();
            }
        }
        
        private static final class NeighborModifier {
            final int addFound;
            final int addNotFound;

            private NeighborModifier(final int addFound, final int addNotFound) {
                this.addFound = addFound;
                this.addNotFound = addNotFound;
            }
        }

        private static final NeighborModifier lower = new NeighborModifier(-1, -1);
        private static final NeighborModifier floor = new NeighborModifier(0, -1);
        private static final NeighborModifier ceil = new NeighborModifier(0, 0);
        private static final NeighborModifier higher = new NeighborModifier(1, 0);

        private static final class Level<K, V> {

            final Memory memory;
            final Level<K, V> nextLevel;
            final Serializer<K> keySerializer;
            final Serializer valueSerializer;
            final Comparator<K> comparator;
            final boolean hasDeletions;
            
            static <K, V> Level<K, V> build(Memory memory, Serializer<K> keySerializer, Serializer<V> valueSerializer, Comparator<K> comparator, boolean hasDeletions, int numLevels) {
                if (numLevels == 0) {
                    return new Level<K, V>(memory, null, keySerializer, valueSerializer, comparator, hasDeletions);
                } else {
                    return new Level<K, V>(memory, build(memory, keySerializer, valueSerializer, comparator, hasDeletions, numLevels - 1), keySerializer, new LongSerializer(), comparator, false);
                }
            }

            Level(Memory memory, @Nullable Level<K, V> nextLevel, Serializer<K> keySerializer, Serializer valueSerializer, Comparator<K> comparator, boolean hasDeletions) {
                this.memory = memory;
                this.nextLevel = nextLevel;
                this.keySerializer = keySerializer;
                this.valueSerializer = valueSerializer;
                this.comparator = comparator;
                this.hasDeletions = hasDeletions;
            }

            boolean isValueLevel() {
                return nextLevel == null;
            }

            DataBlock getBlock(long address) {
                return new DataBlock(address);
            }

            final class DataBlock {

                final int length;
                final long offsetStart;
                final long kvStart;
                final MemoryDataInput in;

                DataBlock(final long blockStart) {
                    length = memory.getInt(blockStart);
                    this.offsetStart = blockStart+4;
                    kvStart = offsetStart+2*length;
                    in = new MemoryDataInput(memory);
                }

                K getKey(int index) {
                    final int offset = memory.getChar(offsetStart+2*index);
                    in.seek(kvStart+offset);
                    try {
                        return keySerializer.read(in);
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }

                Entry<K, Object> getEntry(int index) {
                    final int offset = memory.getChar(offsetStart+2*index);
                    in.seek(kvStart+offset);
                    try {
                        final K key = keySerializer.read(in);
                        final boolean isDeleted = hasDeletions && in.readBoolean();
                        if (isDeleted) {
                            return Entry.createDeleted(key);
                        } else {
                            return Entry.create(key, valueSerializer.read(in));
                        }
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }

                int length() {
                    return length;
                }
                
                @Nullable Entry<K, Object> get(K key) {
                    final int insertionPoint = search(key);
                    if (insertionPoint >= 0) {
                        return getEntry(insertionPoint);
                    }
                    return null;
                }

                int search(K key) {
                    int low = 0;
                    int high = length-1;

                    while (low <= high) {
                        final int mid = (low + high) >>> 1;
                        final K midVal = getKey(mid);
                        final int cmp = comparator.compare(midVal, key);

                        if (cmp < 0) {
                            low = mid + 1;
                        } else if (cmp > 0) {
                            high = mid - 1;
                        } else {
                            return mid;
                        }
                    }
                    return ~low;
                }
            }
        }
    }

    private static class HeaderSerializer implements Serializer<Header> {
        @Override
        public void write(Header header, final DataOutput out) throws IOException {
            out.writeInt(header.indexLevels);
            out.writeLong(header.rootLevelStartAddress);
            out.writeLong(header.valueLevelLength);
            out.writeLong(header.size);
            out.writeByte(header.hasDeletions ? 1 : 0);
            out.writeLong(header.fileLength);
        }

        @Override
        public Header read(final DataInput in) throws IOException {
            final int indexLevels = in.readInt();
            final long rootLevelStartAddress = in.readLong();
            final long valueLevelLength = in.readLong();
            final long size = in.readLong();
            final boolean hasDeletions = in.readByte() != 0;
            final long fileLength = in.readLong();
            return new Header(indexLevels, rootLevelStartAddress, valueLevelLength, size, hasDeletions, fileLength);
        }
    }

    private static class Header {
        int indexLevels;
        long rootLevelStartAddress;
        long valueLevelLength;
        long size;
        boolean hasDeletions;
        long fileLength;

        private Header() {
        }

        private Header(
                final int indexLevels,
                final long rootLevelStartAddress,
                final long valueLevelLength,
                final long size,
                final boolean hasDeletions,
                final long fileLength
        ) {
            this.indexLevels = indexLevels;
            this.rootLevelStartAddress = rootLevelStartAddress;
            this.valueLevelLength = valueLevelLength;
            this.size = size;
            this.hasDeletions = hasDeletions;
            this.fileLength = fileLength;
        }

        public static int length() {
            return 37;
        }

        @Override
        public String toString() {
            return "Header{" +
                    "indexLevels=" +
                    indexLevels +
                    ", rootLevelStartAddress=" +
                    rootLevelStartAddress +
                    ", valueLevelLength=" +
                    valueLevelLength +
                    ", size=" +
                    size +
                    ", hasDeletions=" +
                    hasDeletions +
                    ", fileLength=" +
                    fileLength +
                    '}';
        }
    }
}
