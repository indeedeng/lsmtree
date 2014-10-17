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
