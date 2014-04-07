package com.indeed.lsmtree.recordcache;

import com.indeed.util.compress.CompressionCodec;
import com.indeed.util.compress.SnappyCodec;
import com.indeed.lsmtree.core.StorageType;
import com.indeed.lsmtree.core.Store;
import com.indeed.lsmtree.core.StoreBuilder;
import com.indeed.lsmtree.recordlog.RecordLogDirectory;
import com.indeed.util.serialization.Serializer;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;

/**
 * @author jplaisance
 */
public final class ReplicatingStoreBuilder<K,V> {

    private static final Logger log = Logger.getLogger(ReplicatingStoreBuilder.class);

    private final StoreBuilder<K,V> storeBuilder;

    private final File recordsPath;

    private final File indexPath;

    private final Serializer<K> keySerializer;

    private final Serializer<V> valueSerializer;

    private Serializer<Collection<K>> keyCollectionSerializer;

    private CompressionCodec codec;

    private File checkpointDir;

    public ReplicatingStoreBuilder(File recordsPath, File indexPath, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        storeBuilder = new StoreBuilder<K, V>(indexPath, keySerializer, valueSerializer);
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.recordsPath = recordsPath;
        this.indexPath = indexPath;
    }

    public ReplicatingStoreBuilder<K,V> setKeyCollectionSerializer(final Serializer<Collection<K>> keyCollectionSerializer) {
        this.keyCollectionSerializer = keyCollectionSerializer;
        return this;
    }

    public ReplicatingStoreBuilder<K,V> setCodec(final CompressionCodec codec) {
        this.codec = codec;
        return this;
    }

    public ReplicatingStoreBuilder<K, V> setDedicatedPartition(final boolean dedicatedPartition) {
        storeBuilder.setDedicatedPartition(dedicatedPartition);
        return this;
    }

    public ReplicatingStoreBuilder<K, V> setReservedSpaceThreshold(final long reservedSpaceThreshold) {
        storeBuilder.setReservedSpaceThreshold(reservedSpaceThreshold);
        return this;
    }

    public ReplicatingStoreBuilder<K, V> setComparator(final Comparator<K> comparator) {
        storeBuilder.setComparator(comparator);
        return this;
    }

    public ReplicatingStoreBuilder<K, V> setMaxVolatileGenerationSize(final long maxVolatileGenerationSize) {
        storeBuilder.setMaxVolatileGenerationSize(maxVolatileGenerationSize);
        return this;
    }

    public ReplicatingStoreBuilder<K, V> setStorageType(final StorageType storageType) {
        storeBuilder.setStorageType(storageType);
        return this;
    }

    public ReplicatingStoreBuilder<K,V> setStoreCodec(final CompressionCodec codec) {
        storeBuilder.setCodec(codec);
        return this;
    }

    public ReplicatingStoreBuilder<K,V> setCheckpointDir(final File checkpointDir) {
        this.checkpointDir = checkpointDir;
        return this;
    }

    public RecordLogStore<K, V> build() throws IOException {
        if (codec == null) {
            SnappyCodec snappyCodec = new SnappyCodec();
            codec = snappyCodec;
        }
        if (keyCollectionSerializer == null) {
            keyCollectionSerializer = new CollectionSerializer<K>(keySerializer);
        }
        final Store<K,V> store = storeBuilder.build();
        final RecordLogDirectory recordLogDirectory =
                new RecordLogDirectory.Builder<Operation>(
                        recordsPath,
                        new OperationSerializer(
                                keySerializer,
                                valueSerializer,
                                keyCollectionSerializer
                        ),
                        codec
                ).build();
        final RecordLogDirectoryPoller poller = new RecordLogDirectoryPoller(recordLogDirectory, new File(indexPath, "lastposition.txt"), true, true);
        final File checkpointDir = this.checkpointDir;
        poller.registerFunctions(new RecordLogDirectoryPoller.Functions() {
            @Override
            public void process(final long position, final Operation op) throws IOException {
                if (op instanceof Put) {
                    Put<K,V> put = (Put<K, V>)op;
                    store.put(put.getKey(), put.getValue());
                } else if (op instanceof Delete) {
                    Delete<K> delete = (Delete<K>)op;
                    for (K key : delete.getKeys()) {
                        store.delete(key);
                    }
                } else if (op instanceof Checkpoint) {
                    Checkpoint checkpoint = (Checkpoint)op;
                    if (checkpointDir != null) store.checkpoint(checkpointDir);
                } else {
                    log.error("unknown operation of type "+op.getClass().getName());
                    throw new UnsupportedOperationException();
                }
            }

            @Override
            public void sync() throws IOException {
                store.sync();
            }
        });
        poller.start();

        return new RecordLogStore<K, V>(store, poller, recordLogDirectory);
    }
}
