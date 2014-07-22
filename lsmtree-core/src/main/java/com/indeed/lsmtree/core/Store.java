package com.indeed.lsmtree.core;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.indeed.util.compress.CompressionCodec;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.AtomicSharedReference;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.serialization.Serializer;
import com.indeed.util.core.shell.PosixFileOperations;
import com.indeed.lsmtree.core.iteratee.Enumerator;
import com.indeed.lsmtree.core.iteratee.Input;
import com.indeed.lsmtree.core.iteratee.Iteratee;
import com.indeed.lsmtree.core.iteratee.Processor;
import com.indeed.util.mmap.NativeFileUtils;
import fj.F2;
import fj.P;
import fj.P4;
import fj.data.Stream;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author jplaisance
 */
public final class Store<K, V> implements Closeable {

    private static final Logger log = Logger.getLogger(Store.class);

    private final AtomicSharedReference<GenerationState<K,V>> generationState;

    private final File root;

    private final File dataDir;

    private final AtomicLong lastUsedTimeStamp;

    private final Serializer<K> keySerializer;

    private final Serializer<V> valueSerializer;

    private final Ordering<K> comparator;

    private final long maxVolatileGenerationSize;

    private final Compactor compactor;

    private final File lockFile;

    private final StorageType storageType;

    private final CompressionCodec codec;

    private final AtomicLong totalGenerationSpace = new AtomicLong(0);

    private final AtomicLong reservedCompactionSpace = new AtomicLong(0);

    private final long reservedSpaceThreshold;

    private final boolean mlockFiles;

    private final boolean dedicatedPartition;

    private boolean closed = false;

    private final BloomFilter.MemoryManager memoryManager;

    /**
     * Use {@link StoreBuilder} to create a store.
     *
     * @param root                          root lsm tree index directory
     * @param keySerializer                 key serializer
     * @param valueSerializer               value serializer
     * @param comparator                    key comparator
     * @param maxVolatileGenerationSize     max size of volatile generation in bytes before a compaction should occur
     * @param storageType                   storage type
     * @param codec                         compression codec
     * @param readOnly                      open lsm tree in read only mode
     * @param dedicatedPartition            true if lsm tree is on a dedicated partition
     * @param reservedSpaceThreshold        disk space in bytes that must be available after compactions
     * @param mlockFiles                    mlock files if true
     * @param bloomFilterMemory             memory allocated to bloom filter, in bytes
     * @param mlockBloomFilters             mlock bloom filters if true
     * @throws IOException
     */
    Store(
            File root,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            Comparator<K> comparator,
            long maxVolatileGenerationSize,
            StorageType storageType,
            CompressionCodec codec,
            boolean readOnly,
            final boolean dedicatedPartition,
            final long reservedSpaceThreshold,
            final boolean mlockFiles,
            final long bloomFilterMemory,
            final boolean mlockBloomFilters
    )
            throws IOException {
        this.storageType = storageType;
        this.codec = codec;
        this.dedicatedPartition = dedicatedPartition;
        this.reservedSpaceThreshold = reservedSpaceThreshold;
        this.mlockFiles = mlockFiles;
        if (!root.isDirectory()) {
            if (!root.mkdirs()) {
                final String err = root.getAbsolutePath() + " could not be created";
                log.error(err);
                throw new IOException(err);
            }
        }
        if (!readOnly) {
            final File lockFileLock = new File(root, "write.lock.lock");
            try {
                if (!lockFileLock.createNewFile()) {
                    throw new IOException(lockFileLock.getAbsolutePath()+" is already locked");
                }
                final File lockFile = new File(root, "write.lock");
                if (lockFile.exists()) {
                    final Integer pid = PosixFileOperations.tryParseInt(Files.toString(lockFile, Charsets.UTF_8));
                    if (pid == null || PosixFileOperations.isProcessRunning(pid, true)) {
                        lockFileLock.delete();
                        throw new IOException(lockFile.getAbsolutePath()+" is already locked");
                    }
                }
                Files.write(String.valueOf(PosixFileOperations.getPID()), lockFile, Charsets.UTF_8);
                lockFileLock.delete();
                this.lockFile = lockFile;
                this.lockFile.deleteOnExit();
            } catch (IOException e) {
                log.error("problem locking lsmtree in directory "+root.getAbsolutePath(), e);
                throw e;
            }
        } else {
            lockFile = null;
        }
        this.root = root;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.comparator = Ordering.from(comparator);
        this.maxVolatileGenerationSize = maxVolatileGenerationSize;
        generationState = AtomicSharedReference.create();
        dataDir = new File(root, "data");
        final VolatileGeneration<K,V> nextVolatileGeneration;
        final List<Generation<K,V>> stableGenerations = new ArrayList<Generation<K, V>>();
        final List<File> toDelete = new ArrayList<File>();
        lastUsedTimeStamp = new AtomicLong();
        memoryManager = new BloomFilter.MemoryManager(bloomFilterMemory, mlockBloomFilters);
        try {
            if (!dataDir.exists()) {
                dataDir.mkdirs();
                final File newLog = getNextLogFile();
                nextVolatileGeneration = new VolatileGeneration<K, V>(newLog, keySerializer, valueSerializer, comparator);
            } else {
                long maxTimestamp = 0;
                maxTimestamp = findMaxTimestamp(root, maxTimestamp);
                maxTimestamp = findMaxTimestamp(dataDir, maxTimestamp);
                lastUsedTimeStamp.set(maxTimestamp);
                final File latestDir = new File(root, "latest");
                final File state = new File(latestDir, "state");
                final Yaml yaml = new Yaml();
                final Reader reader = new InputStreamReader(new FileInputStream(state));
                final Map<String, Object> map = (Map<String, Object>)yaml.load(reader);
                Closeables2.closeQuietly(reader, log);
                final File volatileGenerationFile = new File(latestDir, (String)map.get("volatileGeneration"));
                final List<String> oldStableGenerations = (List<String>)map.get("stableGenerations");
                if (readOnly) {
                    nextVolatileGeneration = new VolatileGeneration<K, V>(volatileGenerationFile, keySerializer, valueSerializer, comparator, true);
                    for (String generationName : oldStableGenerations) {
                        final File generationFile = new File(latestDir, generationName);
                        if (generationName.endsWith(".log")) {
                            stableGenerations.add(new VolatileGeneration(getDataFile(generationFile), keySerializer, valueSerializer, comparator, true));
                        } else {
                            stableGenerations.add(StableGeneration.open(
                                    memoryManager,
                                    getDataFile(generationFile),
                                    comparator,
                                    keySerializer,
                                    valueSerializer,
                                    storageType,
                                    codec,
                                    mlockFiles
                            ));
                        }
                    }
                } else {
                    Collections.addAll(toDelete, dataDir.listFiles(new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                            return !oldStableGenerations.contains(name);
                        }
                    }));
                    Collections.addAll(toDelete, root.listFiles(new FileFilter() {
                        @Override
                        public boolean accept(File pathname) {
                            return pathname.isDirectory() && pathname.getName().matches("\\d+");
                        }
                    }));
                    final File newLog = getNextLogFile();
                    nextVolatileGeneration = new VolatileGeneration<K, V>(newLog, keySerializer, valueSerializer, comparator);
                    nextVolatileGeneration.replayTransactionLog(volatileGenerationFile);
                    for (String generationName : oldStableGenerations) {
                        final File generationFile = new File(latestDir, generationName);
                        if (generationName.endsWith(".log")) {
                            final File tempLog = getNextLogFile();
                            final VolatileGeneration temp = new VolatileGeneration(tempLog, keySerializer, valueSerializer, comparator);
                            temp.replayTransactionLog(generationFile);
                            stableGenerations.add(doCompaction(Collections.singletonList((Generation<K, V>)temp), true));
                            temp.delete();
                            toDelete.add(getDataFile(generationFile));
                        } else {
                            stableGenerations.add(StableGeneration.open(
                                    memoryManager,
                                    getDataFile(generationFile),
                                    comparator,
                                    keySerializer,
                                    valueSerializer,
                                    storageType,
                                    codec,
                                    mlockFiles
                            ));
                        }
                    }
                }
            }
            final GenerationState<K,V> nextState;
            final List<SharedReference<? extends Generation<K,V>>> stableGenerationReferences = Lists.newArrayList();
            for (Generation<K, V> generation : stableGenerations) {
                stableGenerationReferences.add(SharedReference.create(generation));
            }
            if (!readOnly) {
                final File checkpointDir = getNextCheckpointDir();
                checkpointDir.mkdirs();
                nextState = new GenerationState<K, V>(stableGenerationReferences, SharedReference.create(nextVolatileGeneration), checkpointDir);
                checkpointGenerationState(nextState, checkpointDir);
                PosixFileOperations.atomicLink(checkpointDir, new File(root, "latest"));
            } else {
                nextState = new GenerationState<K, V>(stableGenerationReferences, SharedReference.create(nextVolatileGeneration), getDataFile(new File(root, "latest")));
            }
            generationState.set(nextState);
            for (Generation<K,V> generation : nextState.stableGenerations) {
                totalGenerationSpace.addAndGet(generation.sizeInBytes());
            }
            if (!readOnly) {
                compactor = new Compactor();
                for (File f : toDelete) {
                    log.info("deleting "+f.getPath());
                    if (f.isDirectory()) {
                        PosixFileOperations.rmrf(f);
                    } else {
                        f.delete();
                    }
                }
            } else {
                if (!toDelete.isEmpty()) log.error("toDelete should be empty");
                compactor = null;
            }
        } catch (Throwable t) {
            memoryManager.close();
            Throwables.propagateIfInstanceOf(t, IOException.class);
            throw Throwables.propagate(t);
        }
    }

    private File getDataFile(File file) {
        return new File(dataDir, file.getName());
    }

    private long findMaxTimestamp(final File dir, long maxTimestamp) {
        for (String str : dir.list()) {
            long timestamp = 0;
            if (str.matches("\\d+")) {
                timestamp = Long.parseLong(str);
            } else if (str.matches("\\d+\\.log")) {
                timestamp = Long.parseLong(str.substring(0, str.length()-4));
            }
            if (timestamp > maxTimestamp) {
                maxTimestamp = timestamp;
            }
        }
        return maxTimestamp;
    }

    private <A, B> A doWithState(F2<GenerationState<K,V>, B, A> function, @Nullable B b) throws IOException {
        final SharedReference<GenerationState<K, V>> localState = generationState.getCopy();
        try {
            if (localState == null) {
                throw new IOException("store is closed");
            }
            return function.f(localState.get(), b);
        } catch (RuntimeIOException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
            log.error("RuntimeIOException inner exception is not IOException", e);
            throw Throwables.propagate(e.getCause());
        } finally {
            Closeables2.closeQuietly(localState, log);
        }
    }

    private <B> void doUntilSuccessful(F2<GenerationState<K,V>, B, Boolean> function, B b) throws IOException {
        while (!doWithState(function, b));
    }

    private final F2<GenerationState<K,V>, K, V> get = new F2<GenerationState<K, V>, K, V>() {
        @Override
        public @Nullable V f(GenerationState<K, V> localState, K key) {
            Generation.Entry<K,V> getResult = localState.volatileGeneration.get(key);
            if (getResult != null) {
                if (getResult.isDeleted()) return null;
                return getResult.getValue();
            }
            for (Generation<K,V> stableGeneration : localState.stableGenerations) {
                getResult = stableGeneration.get(key);
                if (getResult != null) {
                    if (getResult.isDeleted()) return null;
                    return getResult.getValue();
                }
            }
            return null;
        }
    };

    /**
     * Return the value associated with key, or null if no mapping exists.
     *
     * @param key           lookup key
     * @return              value for key, or null if key does not exist
     * @throws IOException  if an I/O error occurs
     */
    public @Nullable V get(K key) throws IOException {
        return doWithState(get, key);
    }

    private final F2<GenerationState<K,V>, K, Boolean> containsKey = new F2<GenerationState<K, V>, K, Boolean>() {
        @Override
        public Boolean f(GenerationState<K, V> localState, K key) {
            Boolean isDeleted = localState.volatileGeneration.isDeleted(key);
            if (isDeleted != null) {
                return isDeleted != Boolean.TRUE;
            }
            for (Generation<K,V> stableGeneration : localState.stableGenerations) {
                isDeleted = stableGeneration.isDeleted(key);
                if (isDeleted != null) {
                    return isDeleted != Boolean.TRUE;
                }
            }
            return false;
        }
    };

    /**
     * Returns true if the key exists in the store.
     *
     * @param key           lookup key
     * @return              true if key exists in store
     * @throws IOException  if an I/O error occurs
     */
    public boolean containsKey(K key) throws IOException {
        return doWithState(containsKey, key);
    }
    
    private final F2<GenerationState<K,V>, Entry<K,V>, Boolean> put = new F2<GenerationState<K, V>, Entry<K,V>, Boolean>() {
        @Override
        public Boolean f(GenerationState<K, V> localState, Entry<K,V> keyValue) {
            try {
                try {
                    localState.volatileGeneration.put(keyValue.getKey(), keyValue.getValue());
                } catch (IOException e) {
                    compactor.compact();
                    throw e;
                }
                if (localState.volatileGeneration.sizeInBytes() > maxVolatileGenerationSize) {
                    compactor.compact();
                }
                return true;
            } catch (TransactionLog.LogClosedException e) {
                return false;
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }
    };

    /**
     * Writes a key/value pair to store, overwriting any existing entry for the key.
     *
     * @param key           key
     * @param value         value
     * @throws IOException  if an I/O exception occurs
     */
    public void put(K key, V value) throws IOException {
        doUntilSuccessful(put, new Entry<K, V>(key, value));
    }
    
    private final F2<GenerationState<K,V>, K, Boolean> delete = new F2<GenerationState<K,V>, K, Boolean>() {
        @Override
        public Boolean f(GenerationState localState, Object key) {
            try {
                try {
                    localState.volatileGeneration.delete(key);
                } catch (IOException e) {
                    compactor.compact();
                    throw e;
                }
                if (localState.volatileGeneration.sizeInBytes() > maxVolatileGenerationSize) {
                    compactor.compact();
                }
                return true;
            } catch (TransactionLog.LogClosedException e) {
                return false;
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }
    };

    /**
     * Removes the mapping for a key.
     *
     * @param key           key to delete
     * @throws IOException
     */
    public void delete(K key) throws IOException {
        doUntilSuccessful(delete, key);
    }
    
    private static <K,V> MergingIterator<K,V> getMergedIterator(GenerationState<K,V> state, Function<Generation<K,V>, Iterator<Generation.Entry<K,V>>> f, Comparator<K> comp) {
        final List<Generation<K,V>> generations = Lists.newArrayList();
        generations.add(state.volatileGeneration);
        generations.addAll(state.stableGenerations);
        return new MergingIterator<K, V>(Lists.transform(generations, f), comp);
    }
    
    @Nullable private static <K,V> Entry<K,V> getFirstNotDeleted(Iterator<Generation.Entry<K,V>> iterator) {
        while (iterator.hasNext()) {
            final Generation.Entry<K, V> next = iterator.next();
            if (!next.isDeleted()) {
                return new Entry<K, V>(next.getKey(), next.getValue());
            }
        }
        return null;
    }
    
    private F2<GenerationState<K,V>, K, Entry<K,V>> neighbor(final boolean reverse, final boolean inclusive) {
        return new F2<GenerationState<K, V>, K, Entry<K, V>>() {

            public @Nullable Entry<K, V> f(GenerationState<K, V> kvGenerationState, final K k) {
                final MergingIterator<K,V> iterator = getMergedIterator(
                        kvGenerationState,
                        new Function<Generation<K, V>, Iterator<Generation.Entry<K, V>>>() {

                            public Iterator<Generation.Entry<K, V>> apply(Generation<K, V> input) {
                                if (k == null) {
                                    return reverse ? input.reverseIterator() : input.iterator();
                                } else {
                                    return reverse ? input.reverseIterator(k, inclusive) : input.iterator(k, inclusive);
                                }
                            }
                        },
                        reverse ? comparator.reverse() : comparator
                );
                return getFirstNotDeleted(iterator);
            }
        };
    }

    /**
     * @param key           lookup key
     * @return              the first entry with key strictly less than specified key, or null if no such entry exists
     * @throws IOException  if an I/O error occurs
     */
    public @Nullable Entry<K,V> lower(final K key) throws IOException {
        return doWithState(neighbor(true, false), key);
    }

    /**
     * @param key           lookup key
     * @return              the first entry with key less than or equal to specified key, or null if no such entry exists
     * @throws IOException  if an I/O error occurs
     */
    public @Nullable Entry<K,V> floor(K key) throws IOException {
        return doWithState(neighbor(true, true), key);
    }

    /**
     * @param key           lookup key
     * @return              the first entry with key greater than or equal to specified key, or null if no such entry exists
     * @throws IOException  if an I/O error occurs
     */
    public @Nullable Entry<K,V> ceil(K key) throws IOException {
        return doWithState(neighbor(false, true), key);
    }

    /**
     * @param key           lookup key
     * @return              the first entry with key strictly greater than specified key, or null if no such entry exists
     * @throws IOException  if an I/O error occurs
     */
    public @Nullable Entry<K,V> higher(K key) throws IOException {
        return doWithState(neighbor(false, false), key);
    }

    /**
     * @return              the entry with lowest key, or null if no such entry exists
     * @throws IOException  if an I/O error occurs
     */
    public @Nullable Entry<K,V> first() throws IOException {
        return doWithState(neighbor(false, false), null);
    }

    /**
     * @return              the entry with highest key, or null if no such entry exists
     * @throws IOException  if an I/O error occurs
     */
    public @Nullable Entry<K,V> last() throws IOException {
        return doWithState(neighbor(true, false), null);
    }

    /**
     * @return              a sorted iterator over all entries in the store
     * @throws IOException  if an I/O error occurs
     */
    public Iterator<Entry<K,V>> iterator() throws IOException {
        return iterator(null, false, false);
    }

    /**
     * Returns a sorted iterator over entries greater than or equal to a specified key.
     * Whether or not keys must be strictly greater is controllable by an inclusive argument.
     *
     * @param start         return entries only greater than this key
     * @param inclusive     if true, include entry if its key is start
     * @return              a sorted iterator over entries fitting the arguments
     * @throws IOException  if an I/O error occurs
     */
    public Iterator<Entry<K,V>> iterator(final K start, final boolean inclusive) throws IOException {
        return iterator(start, inclusive, false);
    }

    /**
     * @return              a reverse sorted iterator over all entries in the store
     * @throws IOException  if an I/O error occurs
     */
    public Iterator<Entry<K,V>> reverseIterator() throws IOException {
        return iterator(null, false, true);
    }

    /**
     * Returns a reverse sorted iterator over entries less than or equal to a specified key.
     * Whether or not keys must be strictly less is controllable by an inclusive argument.
     *
     * @param start         return entries only less than this key
     * @param inclusive     if true, include entry if its key is start
     * @return              a reverse sorted iterator over entries fitting the arguments
     * @throws IOException  if an I/O error occurs
     */
    public Iterator<Entry<K,V>> reverseIterator(final K start, final boolean inclusive) throws IOException {
        return iterator(start, inclusive, true);
    }

    /**
     * Return a sorted iterator over entries starting at a specified key. Whether the iterator is sorted in ascending or
     * descending order and whether the entries need to be strictly greater than a specified key is controllable by arguments.
     *
     * @param start         comparison key
     * @param inclusive     if true, include entries with the key
     * @param reverse       if true, entries will be iterated over in descending order
     * @return              sorted iterator over entries
     * @throws IOException  if an I/O error occurs
     */
    public Iterator<Entry<K,V>> iterator(final @Nullable K start, final boolean inclusive, final boolean reverse) throws IOException {
        return new Iterator<Entry<K, V>>() {
            
            Deque<Entry<K,V>> buffer;

            Processor<Entry<K, V>, Deque<Entry<K, V>>> processor = new Processor<Entry<K, V>, Deque<Entry<K, V>>>() {

                final Input.Matcher<Entry<K,V>, Iteratee<Entry<K,V>, Deque<Entry<K,V>>>> matcher = new Input.Matcher<Entry<K,V>, Iteratee<Entry<K,V>, Deque<Entry<K,V>>>>() {

                    public Iteratee<Entry<K, V>, Deque<Entry<K, V>>> eof() {
                        return Done(buffer);
                    }

                    public Iteratee<Entry<K, V>, Deque<Entry<K, V>>> empty() {
                        return Cont();
                    }

                    public Iteratee<Entry<K, V>, Deque<Entry<K, V>>> element(Entry<K, V> kvEntry) {
                        buffer.add(kvEntry);
                        if (buffer.size() >= 1000) return Done(buffer);
                        return Cont();
                    }
                };

                public Iteratee<Entry<K, V>, Deque<Entry<K, V>>> process(Input<Entry<K, V>> input) {
                    return input.match(matcher);
                }
            };
            
            {
                buffer = new ArrayDeque<Entry<K, V>>(1000);
                if (start == null) {
                    process(processor, reverse);
                } else {
                    process(processor, start, inclusive, reverse);
                }
            }

            public boolean hasNext() {
                return !buffer.isEmpty();
            }

            public Entry<K, V> next() {
                final Entry<K,V> ret = buffer.removeFirst();
                if (buffer.isEmpty()) {
                    try {
                        process(processor, ret.getKey(), false, reverse);
                    } catch (IOException e) {
                        throw new RuntimeIOException(e);
                    }
                }
                return ret;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Uses specified processor to iterate over entries in the store, returning some value determined by the processor.
     *
     * @param processor     processor
     * @param <A>           return type
     * @return              value determined by processor
     * @throws IOException
     */
    public <A> A process(Processor<Entry<K,V>, A> processor) throws IOException {
        return doWithState(this.<A>process(), P.p(processor, (K)null, Boolean.FALSE, Boolean.FALSE));
    }

    public <A> A process(Processor<Entry<K,V>, A> processor, boolean reverse) throws IOException {
        return doWithState(this.<A>process(), P.p(processor, (K)null, Boolean.FALSE, reverse));
    }

    public <A> A process(Processor<Entry<K,V>, A> processor, K start, boolean inclusive, boolean reverse) throws IOException {
        return doWithState(this.<A>process(), P.p(processor, start, inclusive, reverse));
    }

    private <A> Process<A> process() {
        return process;
    }

    private final Process process = new Process();

    private final class Process<A> extends F2<GenerationState<K, V>, P4<Processor<Entry<K, V>, A>, K, Boolean, Boolean>, A> {

        public A f(GenerationState<K, V> kvGenerationState, P4<Processor<Entry<K, V>, A>, K, Boolean, Boolean> p) {
            return Enumerator.runOnce(p._1(), stream(kvGenerationState, p._2(), p._3(), p._4()))._1();
        }
    }

    private Stream<Entry<K,V>> stream(final GenerationState<K, V> state, final K start, final boolean inclusive, final boolean reverse) {
        return Stream.iterableStream(new Iterable<Entry<K, V>>() {
            @Override
            public Iterator<Entry<K, V>> iterator() {

                return new AbstractIterator<Entry<K, V>>() {

                    Iterator<Generation.Entry<K, V>> iterator = getMergedIterator(
                            state,
                            new Function<Generation<K, V>, Iterator<Generation.Entry<K, V>>>() {
                                public Iterator<Generation.Entry<K, V>> apply(Generation<K, V> input) {
                                    if (reverse) {
                                        return start == null ? input.reverseIterator() : input.reverseIterator(start, inclusive);
                                    } else {
                                        return start == null ? input.iterator() : input.iterator(start, inclusive);
                                    }
                                }
                            },
                            reverse ? comparator.reverse() : comparator
                    );

                    @Override
                    protected Entry<K, V> computeNext() {
                        while (iterator.hasNext()) {
                            final Generation.Entry<K, V> next = iterator.next();
                            if (!next.isDeleted()) {
                                return new Entry<K, V>(next.getKey(), next.getValue());
                            }
                        }
                        return endOfData();
                    }
                };
            }
        });
    }

    public void checkpoint(File checkpointDir) throws IOException {
        final SharedReference<GenerationState<K, V>> localState = generationState.getCopy();
        try {
            if (localState == null) {
                throw new IOException("store is closed");
            }
            checkpointDir.mkdirs();
            localState.get().volatileGeneration.checkpoint(checkpointDir);
            for (Generation<K, V> generation : localState.get().stableGenerations) {
                generation.checkpoint(checkpointDir);
            }
            PosixFileOperations.cplr(new File(localState.get().path, "state"), checkpointDir);
        } finally {
            Closeables2.closeQuietly(localState, log);
        }
    }

    /**
     * @return  key comparator
     */
    public Comparator<K> getComparator() {
        return comparator;
    }

    /**
     * @return  key serializer
     */
    public Serializer<K> getKeySerializer() {
        return keySerializer;
    }

    /**
     * @return  value serializer
     */
    public Serializer<V> getValueSerializer() {
        return valueSerializer;
    }

    private File getNextDataFile() throws IOException {
        return new File(dataDir, String.valueOf(getUniqueTimestamp()));
    }

    private File getNextLogFile() throws IOException {
        return new File(dataDir, getUniqueTimestamp()+".log");
    }

    private File getNextCheckpointDir() throws IOException {
        return new File(root, String.valueOf(getUniqueTimestamp()));
    }

    private long getUniqueTimestamp() throws IOException {
        long time;
        long lastUsedTime;
        do {
            time = System.currentTimeMillis();
            lastUsedTime = lastUsedTimeStamp.get();
            if (time <= lastUsedTime) {
                time = lastUsedTime+1;
            }
        } while (!lastUsedTimeStamp.compareAndSet(lastUsedTime, time));
        return time;
    }

    private void checkpointGenerationState(GenerationState<K,V> state, File checkpointDir) throws IOException {
        final Map<String, Object> map = new HashMap<String, Object>();
        final List<String> stableGenerationNames = new ArrayList<String>();
        for (Generation<K,V> generation : state.stableGenerations) {
            final File generationPath = generation.getPath();
            final String generationName = generationPath.getName();
            stableGenerationNames.add(generationName);
            PosixFileOperations.link(generationPath, new File(checkpointDir, generationName));
        }
        map.put("stableGenerations", stableGenerationNames);
        final File volatileGenerationPath = state.volatileGeneration.getPath();
        final String volatileGenerationName = volatileGenerationPath.getName();
        map.put("volatileGeneration", volatileGenerationName);
        PosixFileOperations.link(volatileGenerationPath, new File(checkpointDir, volatileGenerationName));
        final Yaml yaml = new Yaml();
        final String generationStateString = yaml.dump(map);
        RandomAccessFile raf = null;
        FileChannel channel = null;
        try {
            raf = new RandomAccessFile(new File(checkpointDir, "state"), "rw");
            channel = raf.getChannel();
            final byte[] bytes = generationStateString.getBytes(Charsets.UTF_8);
            final ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.remaining() > 0) {
                channel.write(buffer);
            }
            channel.force(true);
        } finally {
            Closeables2.closeQuietly(channel, log);
            Closeables2.closeQuietly(raf, log);
        }
    }

    /**
     * Close the store, clean up lock files.
     *
     * @throws IOException  if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        if (compactor != null) {
            Closeables2.closeQuietly(compactor, log);
        } else {
            Closeables2.closeQuietly(generationState.getAndUnset(), log);
        }
        if (lockFile != null) {
            lockFile.delete();
        }
    }

    /**
     * Flushes volatile generation to disk.
     *
     * @throws IOException  if an I/O error occurs
     */
    public void sync() throws IOException {
        final SharedReference<GenerationState<K, V>> localState = generationState.getCopy();
        try {
            if (localState == null) {
                throw new IOException("store is closed");
            }
            try {
                localState.get().volatileGeneration.sync();
            } catch (IOException e) {
                compactor.compact();
                throw e;
            }
        } finally {
            Closeables2.closeQuietly(localState, log);
        }
    }

    /**
     * Blocks until compactions are complete.
     *
     * @throws InterruptedException
     */
    public void waitForCompactions() throws InterruptedException {
        compactor.waitForCompletion();
    }

    private final F2<GenerationState<K,V>,Object,Long> getActiveSpaceUsage = new F2<GenerationState<K, V>, Object, Long>() {
        @Override
        public Long f(final GenerationState<K, V> state, final Object o) {
            try {
                long spaceUsage = state.volatileGeneration.sizeInBytes();
                for (Generation<K,V> generation : state.stableGenerations) {
                    spaceUsage += generation.sizeInBytes();
                }
                return spaceUsage;
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }
    };

    /**
     * @return              active space in use by all generations, in bytes
     * @throws IOException  if an I/O error occurs
     */
    public long getActiveSpaceUsage() throws IOException {
        return doWithState(getActiveSpaceUsage, null);
    }

    private final F2<GenerationState<K, V>, Object, Long> getTotalSpaceUsage = new F2<GenerationState<K, V>, Object, Long>() {
        @Override
        public Long f(final GenerationState<K, V> state, final Object o) {
            try {
                return totalGenerationSpace.get() + state.volatileGeneration.sizeInBytes();
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }
    };

    /**
     * @return              total space in use, in bytes
     * @throws IOException  if an I/O error occurs
     */
    public long getTotalSpaceUsage() throws IOException {
        return doWithState(getTotalSpaceUsage, null);
    }

    /**
     * @return              space reserved for compaction, in bytes
     * @throws IOException
     */
    public long getReservedSpaceUsage() {
        return reservedCompactionSpace.get();
    }

    /**
     * @return              remaining free space in bytes, excluding any space reserved for compaction and the reserved space threshold
     * @throws IOException
     */
    public long getFreeSpace() throws IOException {
        return getFreeSpace(getReservedSpaceUsage() + reservedSpaceThreshold);
    }

    private long getFreeSpace(long reservedSpace) throws IOException {
        //this formula looks weird at first glance. the difference between actual space used (du) and total space used as tracked internally
        //is already accounted for in the reserved space and shouldn't be counted twice.
        final long tmpSpace = NativeFileUtils.du(root.getCanonicalFile()) - getTotalSpaceUsage();
        return root.getUsableSpace() - reservedSpace + tmpSpace;
    }

    private Generation<K,V> doCompaction(final List<Generation<K, V>> toCompact, boolean hasDeletions)
            throws IOException {
        long spaceToReserve = 0;
        for (Generation<K,V> generation : toCompact) {
            spaceToReserve+=generation.sizeInBytes();
        }
        final long reservedSpace = reservedCompactionSpace.addAndGet(spaceToReserve);
        try {
            if (dedicatedPartition && getFreeSpace(reservedSpace + reservedSpaceThreshold) < 0) {
                throw new IOException("Out of disk space!");
            }
            final File file = getNextDataFile();
            StableGeneration.Writer.write(memoryManager, file, toCompact, keySerializer, valueSerializer, comparator, storageType, codec, hasDeletions);
            final Generation<K, V> generation = StableGeneration.open(memoryManager, file, comparator, keySerializer, valueSerializer, storageType, codec, mlockFiles);
            totalGenerationSpace.addAndGet(generation.sizeInBytes());
            return generation;
        } finally {
            reservedCompactionSpace.addAndGet(-spaceToReserve);
        }
    }

    private final class Compactor implements Closeable {

        final ExecutorService threadPool = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("compaction-thread-%d").setDaemon(true).build());

        final ReentrantLock lock = new ReentrantLock();

        final Condition compactionStateChanged = lock.newCondition();

        final Set<String> currentlyCompacting = new HashSet<String>();

        volatile boolean closed = false;
        
        volatile int runningCompactions = 0;

        public void compact() throws IOException {
            lock.lock();
            try {
                if (!closed) {
                    final SharedReference<GenerationState<K, V>> localStateReference = generationState.getCopy();
                    try {
                        if (localStateReference == null) return;
                        final GenerationState<K, V> localState = localStateReference.get();
                        //this is double checked locking but in this case it doesn't really matter since it's just a heuristic
                        if (localState.volatileGeneration.sizeInBytes() > maxVolatileGenerationSize) {
                            final GenerationState<K, V> nextState = startNewLog(localState);
                            startCompaction(nextState);
                        }
                    } finally {
                        Closeables2.closeQuietly(localStateReference, log);
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        private GenerationState<K, V> startNewLog(final GenerationState<K, V> localState) throws IOException {
            //create new volatile generation and checkpoint
            final File newLog = getNextLogFile();
            final VolatileGeneration<K,V> nextVolatileGeneration = new VolatileGeneration<K, V>(newLog, keySerializer, valueSerializer, comparator);
            final List<SharedReference<? extends Generation<K,V>>> nextStableGenerations = Lists.newArrayList();
            nextStableGenerations.add(localState.volatileGenerationReference.copy());
            for (SharedReference<? extends Generation<K, V>> reference : localState.stableGenerationReferences) {
                nextStableGenerations.add(reference.copy());
            }
            final File checkpointDir = getNextCheckpointDir();
            checkpointDir.mkdirs();
            final GenerationState<K,V> nextState = new GenerationState<K, V>(nextStableGenerations, SharedReference.create(nextVolatileGeneration), checkpointDir);
            checkpointGenerationState(nextState, checkpointDir);
            //there will be a brief period of time where there is no writable generation, put and delete will block during this time
            localState.volatileGeneration.closeWriter();
            PosixFileOperations.atomicLink(checkpointDir, new File(root, "latest"));
            final SharedReference<GenerationState<K, V>> oldState = Preconditions.checkNotNull(generationState.getAndSet(nextState));
            oldState.get().delete();
            Closeables2.closeQuietly(oldState, log);
            return nextState;
        }

        private void startCompaction(final GenerationState<K, V> localState) throws IOException {
            //find generations eligible for compaction and start compaction in background
            final List<SharedReference<? extends Generation<K,V>>> toCompact = Lists.newArrayList();
            long sum = 0;
            boolean hasDeletions = false;
            for (SharedReference<? extends Generation<K, V>> reference : localState.stableGenerationReferences) {
                final Generation<K, V> generation = reference.get();
                final String name = generation.getPath().getName();
                if (!currentlyCompacting.contains(name)) {
                    if ((generation instanceof VolatileGeneration || (sum*2 > generation.sizeInBytes()))) {
                        sum+=generation.sizeInBytes();
                        toCompact.add(reference.copy());
                        currentlyCompacting.add(generation.getPath().getName());
                    } else {
                        hasDeletions = true;
                        break;
                    }
                } else {
                    hasDeletions = true;
                    break;
                }
            }
            if (toCompact.size() > 0) {
                runningCompactions++;
                threadPool.execute(new Compaction(toCompact, hasDeletions));
            }
        }

        private final class Compaction implements Runnable {

            private final List<SharedReference<? extends Generation<K,V>>> toCompact;

            private final boolean hasDeletions;

            private Compaction(List<SharedReference<? extends Generation<K,V>>> toCompact, boolean hasDeletions) {
                this.toCompact = toCompact;
                this.hasDeletions = hasDeletions;
            }

            @Override
            public void run() {
                boolean locked = false;
                final Set<String> compactedGenerations = new HashSet<String>();
                for (SharedReference<? extends Generation<K, V>> generation : toCompact) {
                    compactedGenerations.add(generation.get().getPath().getName());
                }
                try {
                    final List<Generation<K,V>> toCompactGenerations = Lists.newArrayList();
                    for (SharedReference<? extends Generation<K, V>> reference : toCompact) {
                        toCompactGenerations.add(reference.get());
                    }
                    final Generation<K,V> stableGeneration = doCompaction(toCompactGenerations, hasDeletions);
                    lock.lock();
                    locked = true;
                    finishCompaction(compactedGenerations, toCompactGenerations, stableGeneration);
                } catch (Throwable e) {
                    if (!locked) {
                        lock.lock();
                        locked = true;
                    }
                    currentlyCompacting.removeAll(compactedGenerations);
                    log.error("exception during compaction", e);
                    throw Throwables.propagate(e);
                } finally {
                    if (!locked) {
                        lock.lock();
                    }
                    try {
                        for (SharedReference<? extends Generation<K, V>> reference : toCompact) {
                            Closeables2.closeQuietly(reference, log);
                        }
                        runningCompactions--;
                        if (runningCompactions < 0) {
                            log.error("compactions count is "+runningCompactions+", this is bad.");
                        }
                        if (closed) {
                            if (runningCompactions == 0) {
                                try {
                                    finishClose();
                                } catch (IOException e) {
                                    throw Throwables.propagate(e);
                                }
                            }
                        }
                    } finally {
                        compactionStateChanged.signalAll();
                        lock.unlock();
                    }
                }
            }

            private void finishCompaction(final Set<String> compactedGenerations, final List<Generation<K, V>> toCompactGenerations, final Generation<K, V> stableGeneration) throws IOException {
                final List<SharedReference<? extends Generation<K,V>>> nextStableGenerations = Lists.newArrayList();
                final SharedReference<GenerationState<K, V>> stateReference = Preconditions.checkNotNull(generationState.getCopy());
                final GenerationState<K,V> state = stateReference.get();
                try {
                    boolean compactionAdded = false;
                    for (SharedReference<? extends Generation<K, V>> reference : state.stableGenerationReferences) {
                        final String name = reference.get().getPath().getName();
                        if (!compactedGenerations.contains(name)) {
                            nextStableGenerations.add(reference.copy());
                        } else {
                            if (!compactionAdded) {
                                nextStableGenerations.add(SharedReference.create(stableGeneration));
                                compactionAdded = true;
                            }
                            currentlyCompacting.remove(name);
                        }
                    }
                    final File checkpointDir = getNextCheckpointDir();
                    checkpointDir.mkdirs();
                    final GenerationState<K,V> nextState = new GenerationState<K, V>(nextStableGenerations, state.volatileGenerationReference.copy(), checkpointDir);
                    checkpointGenerationState(nextState, checkpointDir);
                    PosixFileOperations.atomicLink(checkpointDir, new File(root, "latest"));
                    final SharedReference<GenerationState<K, V>> oldState = Preconditions.checkNotNull(generationState.getAndSet(nextState));
                    oldState.get().delete();
                    Closeables2.closeQuietly(oldState, log);
                    for (Generation<K, V> generation : toCompactGenerations) {
                        final long sizeInBytes = generation.sizeInBytes();
                        generation.delete();
                        totalGenerationSpace.addAndGet(-sizeInBytes);
                    }
                } finally {
                    Closeables2.closeQuietly(stateReference, log);
                }
            }
        }

        @Override
        public void close() throws IOException {
            lock.lock();
            try {
                closed = true;
                if (runningCompactions == 0) {
                    finishClose();
                }
            } finally {
                compactionStateChanged.signalAll();
                lock.unlock();
            }
        }

        private void finishClose() throws IOException {
            try {
                final SharedReference<GenerationState<K, V>> state = generationState.getAndUnset();
                try {
                    if (state != null) {
                        final VolatileGeneration<K, V> volatileGeneration = state.get().volatileGeneration;
                        if (volatileGeneration != null) volatileGeneration.closeWriter();
                    }
                } finally {
                    Closeables2.closeQuietly(state, log);
                }
            } finally {
                threadPool.shutdown();
            }
        }

        public void waitForCompletion() throws InterruptedException {
            while (true) {
                lock.lock();
                try {
                    if (closed && runningCompactions == 0) {
                        return;
                    }
                    compactionStateChanged.await();
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    private static final class GenerationState<K, V> implements Closeable {
        private final List<SharedReference<? extends Generation<K,V>>> stableGenerationReferences;
        private final SharedReference<VolatileGeneration<K,V>> volatileGenerationReference;
        private final List<Generation<K, V>> stableGenerations;
        private final VolatileGeneration<K,V> volatileGeneration;

        private final File path;

        public GenerationState(
                final List<SharedReference<? extends Generation<K, V>>> stableGenerationReferences,
                final SharedReference<VolatileGeneration<K, V>> volatileGenerationReference,
                File path
        ) {
            this.path = path;
            this.stableGenerationReferences = ImmutableList.copyOf(stableGenerationReferences);
            this.volatileGenerationReference = volatileGenerationReference;
            this.volatileGeneration = volatileGenerationReference.get();
            final ImmutableList.Builder<Generation<K,V>> builder = ImmutableList.builder();
            for (SharedReference<? extends Generation<K, V>> generation : stableGenerationReferences) {
                builder.add(generation.get());
            }
            stableGenerations = builder.build();
        }

        public void delete() throws IOException {
            log.info("deleting "+path);
            PosixFileOperations.rmrf(path);
        }

        @Override
        public void close() throws IOException {
            Closeables2.closeQuietly(volatileGenerationReference, log);
            for (SharedReference<? extends Generation<K, V>> reference : stableGenerationReferences) {
                Closeables2.closeQuietly(reference, log);
            }
        }
    }

    /**
     * A key/value pair.
     *
     * @param <K>   key type
     * @param <V>   value type
     */
    public static final class Entry<K,V> {
        private final K key;
        private final V value;

        public Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }
}
