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

import com.indeed.util.serialization.Serializer;
import fj.P1;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

/**
 * @author jplaisance
 */
public final class OperationSerializer<K,V> implements Serializer<Operation> {

    private static final Logger log = Logger.getLogger(OperationSerializer.class);

    private final Serializer<K> keySerializer;

    private final Serializer<V> valueSerializer;

    private final Serializer<Collection<K>> keyCollectionSerializer;

    public OperationSerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(keySerializer, valueSerializer, new CollectionSerializer<K>(keySerializer));
    }

    public OperationSerializer(final Serializer<K> keySerializer, final Serializer<V> valueSerializer, Serializer<Collection<K>> keyCollectionSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.keyCollectionSerializer = keyCollectionSerializer;
    }

    @Override
    public void write(final Operation operation, final DataOutput out) throws IOException {
        if (operation.getClass() == Put.class) {
            out.writeByte(1);
            Put<K,V> put = (Put)operation;
            keySerializer.write(put.getKey(), out);
            valueSerializer.write(put.getValue(), out);
        } else if (operation.getClass() == Delete.class) {
            out.writeByte(2);
            Delete<K> delete = (Delete)operation;
            keyCollectionSerializer.write(delete.getKeys(), out);
        } else if (operation.getClass() == Checkpoint.class) {
            Checkpoint checkpoint = (Checkpoint)operation;
            out.writeByte(3);
            out.writeLong(checkpoint.getTimestamp());
        } else {
            throw new RuntimeException("type does not match any of available types");
        }
    }

    @Override
    public Operation read(final DataInput dataIn) throws IOException {
        try {
            int type = dataIn.readByte();
            if (type == 1) {
                return new Put(keySerializer.read(dataIn), new P1<V>() {

                    V v = null;
                    boolean initialized = false;
                    @Override
                    public synchronized V _1() {
                        if (initialized) return v;
                        initialized = true;
                        try {
                            v = valueSerializer.read(dataIn);
                            return v;
                        } catch (IOException e) {
                            log.error("error parsing value", e);
                            throw new RuntimeException(e);
                        }
                    }
                });
            } else if (type == 2) {
                return new Delete(keyCollectionSerializer.read(dataIn));
            } else if (type == 3) {
                return new Checkpoint(dataIn.readLong());
            } else {
                throw new RuntimeException("type does not match any of available types");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
