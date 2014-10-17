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
 package com.indeed.lsmtree.core;

import com.indeed.util.serialization.IntSerializer;
import com.indeed.util.serialization.LongSerializer;
import junit.framework.TestCase;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
* @author jplaisance
*/
public final class TestZeroData extends TestCase {

    private static final Logger log = Logger.getLogger(TestZeroData.class);

    File tmpDir;

    @Override
    public void setUp() throws Exception {
        tmpDir = File.createTempFile("tmp", "", new File("."));
        tmpDir.delete();
        tmpDir.mkdirs();
    }

    @Override
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(tmpDir);
    }

    public void testEmpty() throws IOException {
        VolatileGeneration<Integer, Long> generation = new VolatileGeneration<Integer, Long>(new File(tmpDir, "log.log"), new IntSerializer(), new LongSerializer(), new ComparableComparator());
        final File indexDir = new File(tmpDir, "indexdir");
        ImmutableBTreeIndex.Writer.write(indexDir, generation.iterator(), new IntSerializer(), new LongSerializer(), 65536, false);
        ImmutableBTreeIndex.Reader<Integer, Long> reader = new ImmutableBTreeIndex.Reader<Integer, Long>(indexDir, new IntSerializer(), new LongSerializer(), false);
        final Iterator<Generation.Entry<Integer, Long>> iterator = reader.iterator();
        assertFalse(iterator.hasNext());
    }

    public void testAllDeletes() throws IOException, TransactionLog.LogClosedException {
        VolatileGeneration<Integer, Long> generation = new VolatileGeneration<Integer, Long>(new File(tmpDir, "log.log"), new IntSerializer(), new LongSerializer(), new ComparableComparator());
        for (int i = 0; i < 10000; i++) {
            generation.delete(i);
        }
        final File indexDir = new File(tmpDir, "indexdir");
        ImmutableBTreeIndex.Writer.write(indexDir, generation.iterator(), new IntSerializer(), new LongSerializer(), 65536, false);
        ImmutableBTreeIndex.Reader<Integer, Long> reader = new ImmutableBTreeIndex.Reader<Integer, Long>(indexDir, new IntSerializer(), new LongSerializer(), false);
        final Iterator<Generation.Entry<Integer, Long>> iterator = reader.iterator();
        assertFalse(iterator.hasNext());
        for (int i = 0; i < 10000; i++) {
            assertNull(reader.get(i));
        }
    }

    public void testAllDeletedInYoungGeneration() throws IOException, TransactionLog.LogClosedException {
        VolatileGeneration<Integer, Long> generation = new VolatileGeneration<Integer, Long>(new File(tmpDir, "log1.log"), new IntSerializer(), new LongSerializer(), new ComparableComparator());
        for (int i = 0; i < 10000; i++) {
            generation.put(i, (long)i);
        }
        final File indexDir1 = new File(tmpDir, "indexdir1");
        ImmutableBTreeIndex.Writer.write(indexDir1, generation.iterator(), new IntSerializer(), new LongSerializer(), 65536, false);
        ImmutableBTreeIndex.Reader<Integer, Long> reader = new ImmutableBTreeIndex.Reader<Integer, Long>(indexDir1, new IntSerializer(), new LongSerializer(), false);
        generation = new VolatileGeneration<Integer, Long>(new File(tmpDir, "log2.log"), new IntSerializer(), new LongSerializer(), new ComparableComparator());
        for (int i = 0; i < 10000; i++) {
            generation.delete(i);
        }
        final File indexDir2 = new File(tmpDir, "indexDir2");
        MergingIterator<Integer, Long> merged = new MergingIterator<Integer, Long>(Arrays.asList(generation.iterator(), reader.iterator()), new ComparableComparator());
        ImmutableBTreeIndex.Writer.write(indexDir2, merged, new IntSerializer(), new LongSerializer(), 65536, false);
        ImmutableBTreeIndex.Reader<Integer, Long> reader2 = new ImmutableBTreeIndex.Reader<Integer, Long>(indexDir2, new IntSerializer(), new LongSerializer(), false);
        Iterator<Generation.Entry<Integer, Long>> iterator = reader2.iterator();
        assertFalse(iterator.hasNext());
        for (int i = 0; i < 10000; i++) {
            assertNull(reader2.get(i));
        }
    }
}
