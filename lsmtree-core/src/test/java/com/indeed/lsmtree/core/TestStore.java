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

import com.indeed.util.compress.CompressionCodec;
import com.indeed.util.compress.SnappyCodec;
import com.indeed.util.core.shell.PosixFileOperations;
import com.indeed.util.serialization.IntSerializer;
import com.indeed.util.serialization.LongSerializer;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
* @author jplaisance
*/
public final class TestStore extends TestCase {

    private static final Logger log = Logger.getLogger(TestStore.class);

    private static final int SMALL_TREE_SIZE = 10000;
    private static final int LARGE_TREE_SIZE = 10000000;

    File tmpDir;

    int treeSize = SMALL_TREE_SIZE;

    @Override
    public void setUp() throws Exception {
        tmpDir = File.createTempFile("tmp", "", new File("."));
        tmpDir.delete();
        tmpDir.mkdirs();
        String treeSizeStr = System.getProperty("lsmtree.test.size");
        treeSize = "large".equals(treeSizeStr) ? LARGE_TREE_SIZE : SMALL_TREE_SIZE;
    }

    @Override
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(tmpDir);
    }

    public void testInline() throws Exception {
        testStore(StorageType.INLINE, null);
    }

    public void testBlockCompressed() throws Exception {
        final SnappyCodec codec = new SnappyCodec();
        testStore(StorageType.BLOCK_COMPRESSED, codec);
    }

    public void testStore(StorageType storageType, CompressionCodec codec) throws Exception {
        File indexDir = new File(tmpDir, "index");
        indexDir.mkdirs();
        File indexLink = new File(tmpDir, "indexlink");
        PosixFileOperations.link(indexDir, indexLink);
        File storeDir = new File(indexLink, "store");
        Store<Integer, Long> store = new StoreBuilder<Integer, Long>(storeDir, new IntSerializer(), new LongSerializer()).setMaxVolatileGenerationSize(8*1024*1024).setStorageType(storageType).setCodec(codec).build();
        final Random r = new Random(0);
        final int[] ints = new int[treeSize];
        for (int i = 0; i < ints.length; i++) {
            ints[i] = r.nextInt();
        }
        for (final int i : ints) {
            store.put(i, (long)i);
            assertTrue(store.get(i) == i);
        }
        for (final int i : ints) {
            assertTrue(store.get(i) == i);
        }
        store.close();
        store.waitForCompactions();
        store = new StoreBuilder<Integer, Long>(storeDir, new IntSerializer(), new LongSerializer()).setMaxVolatileGenerationSize(8*1024*1024).setStorageType(storageType).setCodec(codec).build();
        Arrays.sort(ints);
        Iterator<Store.Entry<Integer, Long>> iterator = store.iterator();
        int index = 0;
        while (iterator.hasNext()) {
            Store.Entry<Integer, Long> next = iterator.next();
            int current = ints[index];
            assertTrue(next.getKey() == ints[index]);
            assertTrue(next.getValue() == ints[index]);
            while (index < ints.length && ints[index] == current) {
                index++;
            }
        }
        assertTrue(index == ints.length);
        final BitSet deleted = new BitSet();
        for (int i = 0; i < ints.length/10; i++) {
            int deletionIndex = r.nextInt(ints.length);
            deleted.set(deletionIndex, true);
            for (int j = deletionIndex-1; j >= 0; j--) {
                if (ints[j] == ints[deletionIndex]) {
                    deleted.set(j, true);
                } else {
                    break;
                }
            }
            for (int j = deletionIndex+1; j < ints.length; j++) {
                if (ints[j] == ints[deletionIndex]) {
                    deleted.set(j, true);
                } else {
                    break;
                }
            }
            store.delete(ints[deletionIndex]);
            assertNull(store.get(ints[deletionIndex]));
        }
        iterator = store.iterator();
        index = 0;
        while (iterator.hasNext()) {
            Store.Entry<Integer, Long> next = iterator.next();
            while (deleted.get(index)) index++;
            int current = ints[index];
            assertTrue(next.getKey() == ints[index]);
            assertTrue(next.getValue() == ints[index]);
            while (index < ints.length && ints[index] == current) {
                index++;
            }
        }
        while (deleted.get(index)) index++;
        assertTrue(index == ints.length);
        final int max = ints[ints.length-1];
        final AtomicInteger done = new AtomicInteger(8);
        for (int i = 0; i < done.get(); i++) {
            final int thread = i;
            final Store<Integer, Long> finalStore = store;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Random r = new Random(thread);
                        for (int i = 0; i < treeSize; i++) {
                            int rand = r.nextInt();
                            int insertionindex = Arrays.binarySearch(ints, rand);

                            Store.Entry<Integer, Long> next = finalStore.ceil(rand);
                            boolean found = next != null;
                            if (insertionindex >= 0 && deleted.get(insertionindex)) {
                                assertNull(finalStore.get(ints[insertionindex]));
                            } else {
                                assertTrue(found == (rand <= max));
                                if (found) {
                                    assertTrue(next.getKey() >= rand);
                                    assertTrue(next.getKey().longValue() == next.getValue());
                                    if (insertionindex >= 0) {
                                        assertTrue(rand == ints[insertionindex]);
                                        assertTrue(next.getKey() == rand);
                                        Long result = finalStore.get(rand);
                                        assertTrue(result == rand);
                                    } else {
                                        int nextIndex = ~insertionindex;
                                        while (deleted.get(nextIndex) && nextIndex < ints.length) nextIndex++;
                                        if (nextIndex < ints.length) {
                                            if (insertionindex != -1) assertTrue(ints[(~insertionindex)-1] < rand);
                                            assertTrue(ints[nextIndex]+" != "+next.getKey(), ints[nextIndex] == next.getKey());

                                        }
                                        Long result = finalStore.get(rand);
                                        assertTrue(result == null);
                                    }
                                }
                            }
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        done.decrementAndGet();
                    }
                }
            }).start();
        }
        while (done.get() > 0) {
            Thread.yield();
        }
        store.close();
    }
}
