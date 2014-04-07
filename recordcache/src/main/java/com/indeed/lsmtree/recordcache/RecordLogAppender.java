package com.indeed.lsmtree.recordcache;

import com.indeed.util.compress.CompressionCodec;
import com.indeed.lsmtree.recordlog.GenericRecordLogAppender;
import com.indeed.util.serialization.Serializer;
import fj.P;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jplaisance
 */
public final class RecordLogAppender<K,V> extends GenericRecordLogAppender<Operation> implements IRecordLogAppender<K, V> {

    private final Comparator<K> keyComparator;

    public RecordLogAppender(File file, Serializer<K> keySerializer, Serializer<V> valueSerializer, Comparator<K> keyComparator, CompressionCodec codec)
            throws IOException {
        this(file, keySerializer, valueSerializer, keyComparator, codec, null);
    }

    public RecordLogAppender(File file, Serializer<K> keySerializer, Serializer<V> valueSerializer, Comparator<K> keyComparator, CompressionCodec codec, AtomicReference<Map<String, String>> metadataRef)
            throws IOException {
        this(file, keySerializer, valueSerializer, new CollectionSerializer<K>(keySerializer), keyComparator, codec, metadataRef);
    }

    public RecordLogAppender(File file, Serializer<K> keySerializer, Serializer<V> valueSerializer, Serializer<Collection<K>> keyCollectionSerializer, Comparator<K> keyComparator, CompressionCodec codec) throws IOException {
        this(file, keySerializer, valueSerializer, keyCollectionSerializer, keyComparator, codec, null);
    }

    public RecordLogAppender(File file, Serializer<K> keySerializer, Serializer<V> valueSerializer, Serializer<Collection<K>> keyCollectionSerializer, Comparator<K> keyComparator, CompressionCodec codec, AtomicReference<Map<String, String>> metadataRef) throws IOException {
        super(file, new OperationSerializer<K, V>(keySerializer, valueSerializer, keyCollectionSerializer), codec, metadataRef);
        this.keyComparator = keyComparator;
    }

    @Override
    public void deleteDocs(final Collection<K> ids) throws IOException {
        ArrayList<K> sorted = new ArrayList<K>(ids);
        Collections.sort(sorted, keyComparator);
        writeOperation(new Delete(sorted));
    }

    @Override
    public long write(final K key, final V value) throws IOException {
        return writeOperation(new Put<K, V>(key, P.p(value)));
    }

    public void putCheckpoint(long timestamp) throws IOException {
        writeOperation(new Checkpoint(timestamp));
    }
}
