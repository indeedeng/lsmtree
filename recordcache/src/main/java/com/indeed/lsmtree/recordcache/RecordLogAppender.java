/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    /**
     * @param file              record log directory
     * @param keySerializer     key serializer
     * @param valueSerializer   value serializer
     * @param keyComparator     key comparator
     * @param codec             compression codec
     * @throws IOException
     */
    public RecordLogAppender(File file, Serializer<K> keySerializer, Serializer<V> valueSerializer, Comparator<K> keyComparator, CompressionCodec codec)
            throws IOException {
        this(file, keySerializer, valueSerializer, keyComparator, codec, null);
    }

    /**
     * @param file              record log directory
     * @param keySerializer     key serializer
     * @param valueSerializer   value serializer
     * @param keyComparator     key comparator
     * @param codec             compression codec
     * @param metadataRef       atomic reference to a metadata object, if nonnull after construction it will contain a reference to previous flushed metadata if it exists
     * @throws IOException
     */
    public RecordLogAppender(File file, Serializer<K> keySerializer, Serializer<V> valueSerializer, Comparator<K> keyComparator, CompressionCodec codec, AtomicReference<Map<String, String>> metadataRef)
            throws IOException {
        this(file, keySerializer, valueSerializer, new CollectionSerializer<K>(keySerializer), keyComparator, codec, metadataRef);
    }

    /**
     * @param file                      record log directory
     * @param keySerializer             key serializer
     * @param valueSerializer           value serializer
     * @param keyCollectionSerializer   key collection serializer
     * @param keyComparator             key comparator
     * @param codec                     compression codec
     * @throws IOException
     */
    public RecordLogAppender(File file, Serializer<K> keySerializer, Serializer<V> valueSerializer, Serializer<Collection<K>> keyCollectionSerializer, Comparator<K> keyComparator, CompressionCodec codec) throws IOException {
        this(file, keySerializer, valueSerializer, keyCollectionSerializer, keyComparator, codec, null);
    }

    /**
     * @param file                      record log directory
     * @param keySerializer             key serializer
     * @param valueSerializer           value serializer
     * @param keyCollectionSerializer   key collection serializer
     * @param keyComparator             key comparator
     * @param codec                     compression codec
     * @param metadataRef               atomic reference to a metadata object, if nonnull after construction it will contain a reference to previous flushed metadata if it exists
     * @throws IOException
     */
    public RecordLogAppender(File file, Serializer<K> keySerializer, Serializer<V> valueSerializer, Serializer<Collection<K>> keyCollectionSerializer, Comparator<K> keyComparator, CompressionCodec codec, AtomicReference<Map<String, String>> metadataRef) throws IOException {
        super(file, new OperationSerializer<K, V>(keySerializer, valueSerializer, keyCollectionSerializer), codec, metadataRef);
        this.keyComparator = keyComparator;
    }

    /**
     * Marks keys as deleted.
     *
     * @param ids           keys to delete
     * @throws IOException
     */
    @Override
    public void deleteDocs(final Collection<K> ids) throws IOException {
        ArrayList<K> sorted = new ArrayList<K>(ids);
        Collections.sort(sorted, keyComparator);
        writeOperation(new Delete(sorted));
    }

    /**
     * Writes a new key/value entry. Can be used to overwrite previous existing values.
     *
     * @param key       key to write
     * @param value     value to write
     * @return          position written
     * @throws IOException
     */
    @Override
    public long write(final K key, final V value) throws IOException {
        return writeOperation(new Put<K, V>(key, P.p(value)));
    }

    public void putCheckpoint(long timestamp) throws IOException {
        writeOperation(new Checkpoint(timestamp));
    }
}
