package com.indeed.lsmtree.recordlog;

import com.indeed.util.io.Syncable;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author jplaisance
 */
public interface RecordFile<E> extends Closeable {
    public E get(long address) throws IOException;
    public Reader<E> reader() throws IOException;
    public Reader<E> reader(long address) throws IOException;

    public interface Writer<E> extends Closeable, Syncable {
        public long append(E e) throws IOException;
    }

    public interface Reader<E> extends Closeable {
        public boolean next() throws IOException;
        public long getPosition();
        public E get();
    }
}
