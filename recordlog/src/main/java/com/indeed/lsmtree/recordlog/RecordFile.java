package com.indeed.lsmtree.recordlog;

import com.indeed.util.io.Syncable;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author jplaisance
 */
public interface RecordFile<E> extends Closeable {
    /**
     * @param address   position
     * @return          value at address
     * @throws IOException
     */
    public E get(long address) throws IOException;

    /**
     * @return          reader starting at address 0
     * @throws IOException
     */
    public Reader<E> reader() throws IOException;

    /**
     * @param address   position
     * @return          reader seeked to provided address
     * @throws IOException
     */
    public Reader<E> reader(long address) throws IOException;

    public interface Writer<E> extends Closeable, Syncable {
        /**
         * Appends entry to the file
         *
         * @param entry     entry to write
         * @return          address written to
         * @throws IOException
         */
        public long append(E entry) throws IOException;
    }

    public interface Reader<E> extends Closeable {
        /**
         * Seeks to next entry
         *
         * @return  true if an entry exists
         * @throws IOException
         */
        public boolean next() throws IOException;

        /**
         * @return  position of current entry
         */
        public long getPosition();

        /**
         * @return  value of current entry
         */
        public E get();
    }
}
