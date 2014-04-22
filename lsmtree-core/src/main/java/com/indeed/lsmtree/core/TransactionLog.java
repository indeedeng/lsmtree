package com.indeed.lsmtree.core;

import com.google.common.io.Closer;
import com.indeed.util.serialization.Serializer;
import com.indeed.lsmtree.recordlog.BasicRecordFile;
import com.indeed.lsmtree.recordlog.RecordFile;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

/**
 * @author jplaisance
 */
public final class TransactionLog {

    private static final Logger log = Logger.getLogger(TransactionLog.class);

    public static class Reader<K, V> implements Closeable {

        private final BasicRecordFile<OpKeyValue<K, V>> recordFile;
        private final RecordFile.Reader<OpKeyValue<K,V>> reader;
        private boolean done;
        private TransactionType type;
        private K key;
        private V value;

        public Reader(File path, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
            recordFile = new BasicRecordFile<OpKeyValue<K, V>>(path, new OpKeyValueSerialzer<K, V>(keySerializer, valueSerializer));
            reader = recordFile.reader();
        }

        public boolean next() throws IOException {
            if (done) return false;
            try {
                if (!reader.next()) {
                    done = true;
                    return false;
                }
            } catch (IOException e) {
                log.warn("error reading log file, halting log replay at this point", e);
                done = true;
                return false;
            }
            final OpKeyValue<K,V> opKeyValue = reader.get();
            type = opKeyValue.type;
            key = opKeyValue.key;
            if (type == TransactionType.PUT) {
                value = opKeyValue.value;
            }
            return true;
        }

        public TransactionType getType() {
            return type;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        @Override
        public void close() throws IOException {
            final Closer closer = Closer.create();
            closer.register(reader);
            closer.register(recordFile);
            closer.close();
        }
    }

    public static class Writer<K, V> implements Closeable {

        private final BasicRecordFile.Writer<OpKeyValue<K, V>> writer;

        private boolean sync;
        private boolean isClosed = false;
        private long sizeInBytes = 0;

        public Writer(File file, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
            this(file, keySerializer, valueSerializer, true);
        }

        public Writer(File file, Serializer<K> keySerializer, Serializer<V> valueSerializer, boolean sync) throws IOException {
            this.sync = sync;
            writer = new BasicRecordFile.Writer(file, new OpKeyValueSerialzer(keySerializer, valueSerializer));
        }

        public synchronized void put(K key, V value) throws IOException, LogClosedException {
            if (isClosed) {
                throw new LogClosedException();
            }
            try {
                sizeInBytes = writer.append(new OpKeyValue<K, V>(TransactionType.PUT, key, value));
                if (sync) {
                    writer.sync();
                }
            } catch (IOException e) {
                close();
                throw e;
            }
        }

        public synchronized void delete(K key) throws IOException, LogClosedException {
            if (isClosed) {
                throw new LogClosedException();
            }
            try {
                sizeInBytes = writer.append(new OpKeyValue<K, V>(TransactionType.DELETE, key, null));
                if (sync) {
                    writer.sync();
                }
            } catch (IOException e) {
                close();
                throw e;
            }
        }

        /**
         * Try to clean up all this crap, if any of it throws an exception then rethrow one of them at the end.
         * @throws IOException
         */
        public synchronized void close() throws IOException {
            if (!isClosed) {
                IOException exception = null;
                try {
                    writer.sync();
                } catch (IOException e) {
                    exception = e;
                }
                try {
                    writer.close();
                } catch (IOException e) {
                    exception = e;
                }
                isClosed = true;
                if (exception != null) {
                    throw exception;
                }
            }
        }

        public synchronized long sizeInBytes() throws IOException {
            return sizeInBytes;
        }

        public synchronized void sync() throws IOException {
            try {
                if (!isClosed) writer.sync();
            } catch (IOException e) {
                close();
                throw e;
            }
        }
    }

    public static enum TransactionType {
        PUT(1),
        DELETE(2);

        int transactionTypeId;

        public int getTransactionTypeId() {
            return transactionTypeId;
        }

        TransactionType(final int transactionTypeId) {
            this.transactionTypeId = transactionTypeId;
        }

        public static TransactionType getTransactionType(int transactionTypeId) {
            switch(transactionTypeId) {
                case 1 : return PUT;
                case 2 : return DELETE;
                default : throw new IllegalArgumentException(transactionTypeId+" is not a valid transactionTypeId");
            }
        }
    }
    
    private static final class OpKeyValue<K, V> {
        TransactionType type;
        K key;
        V value;

        private OpKeyValue(TransactionType type, K key, @Nullable V value) {
            this.type = type;
            this.key = key;
            this.value = value;
        }
    }
    
    private static final class OpKeyValueSerialzer<K,V> implements Serializer<OpKeyValue<K,V>> {
        
        Serializer<K> keySerializer;
        Serializer<V> valueSerializer;

        private OpKeyValueSerialzer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        @Override
        public void write(OpKeyValue<K, V> kvOpKeyValue, DataOutput out) throws IOException {
            out.writeByte(kvOpKeyValue.type.getTransactionTypeId());
            keySerializer.write(kvOpKeyValue.key, out);
            if (kvOpKeyValue.type ==TransactionType.PUT) {
                valueSerializer.write(kvOpKeyValue.value, out);
            }
        }

        @Override
        public OpKeyValue<K, V> read(DataInput in) throws IOException {
            final TransactionType type = TransactionType.getTransactionType(in.readByte());
            final K key = keySerializer.read(in);
            V value = null;
            if (type == TransactionType.PUT) {
                value = valueSerializer.read(in);
            }
            return new OpKeyValue<K, V>(type, key, value);
        }
    }

    public static class LogClosedException extends Exception {}
}
