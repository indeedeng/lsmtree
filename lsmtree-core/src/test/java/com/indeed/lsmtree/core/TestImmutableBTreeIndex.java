package com.indeed.lsmtree.core;

import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Maps;
import com.indeed.util.serialization.IntSerializer;
import com.indeed.util.serialization.LongSerializer;
import com.indeed.util.serialization.StringSerializer;
import junit.framework.TestCase;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jplaisance
 */
public final class TestImmutableBTreeIndex extends TestCase {

    private static final Logger log = Logger.getLogger(TestImmutableBTreeIndex.class);

    private static final int SMALL_TREE_SIZE = 100000;
    private static final int LARGE_TREE_SIZE = 100000000;

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

    public int[] createTree() throws IOException, InstantiationException, IllegalAccessException {
        Random r = new Random(0);
        final int[] ints = new int[treeSize];
        int previous = 0;
        for (int i = 0; i < ints.length; i++) {
            ints[i] = previous+r.nextInt(10)+1;
            previous = ints[i];
        }
        Iterator<Generation.Entry<Integer, Long>> iterator = new Iterator<Generation.Entry<Integer, Long>>() {

            int index = 0;
            
            @Override
            public boolean hasNext() {
                return index < ints.length;
            }

            @Override
            public Generation.Entry<Integer, Long> next() {
                int ret = ints[index];
                index++;
                return Generation.Entry.create(ret, (long) ret);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
        ImmutableBTreeIndex.Writer.write(tmpDir, iterator, new IntSerializer(), new LongSerializer(), 65536, false);
        return ints;
    }

    public void testSequential() throws Exception {
        int[] ints = createTree();
        ImmutableBTreeIndex.Reader<Integer, Long> reader = new ImmutableBTreeIndex.Reader(tmpDir, new IntSerializer(), new LongSerializer(), false);
        Iterator<Generation.Entry<Integer, Long>> iterator = reader.iterator();
        int index = 0;
        while (iterator.hasNext()) {
            Generation.Entry<Integer, Long> next = iterator.next();
            assertTrue(next.getKey() == ints[index]);
            assertTrue(next.getValue() == ints[index]);
            index++;
        }
        assertTrue(index == ints.length);
        for (final int i : ints) {
            Generation.Entry<Integer, Long> entry = reader.get(i);
            assertNotNull(entry);
            assertTrue(entry.getKey() == i);
            assertTrue(entry.getValue() == i);
            assertTrue(reader.get(i).getValue() == i);
        }
        assertNull(reader.get(ints[ints.length - 1] + 1));
    }

    public void testRandom() throws Exception {
        final int[] ints = createTree();
        final ImmutableBTreeIndex.Reader<Integer, Long> reader = new ImmutableBTreeIndex.Reader(tmpDir, new IntSerializer(), new LongSerializer(), false);
        final int max = ints[ints.length-1];
        final AtomicInteger done = new AtomicInteger(8);
        for (int i = 0; i < 8; i++) {
            final int index = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        final Random r = new Random(index);
                        for (int i = 0; i < treeSize; i++) {
                            int rand = r.nextInt(max+1);
                            int insertionindex = Arrays.binarySearch(ints, rand);
                            final Iterator<Generation.Entry<Integer, Long>> iterator = reader.iterator(rand, true);
                            try {
                                assertTrue(iterator.hasNext());
                            } catch (Throwable t) {
                                System.err.println("rand: "+rand);
                                throw Throwables.propagate(t);
                            }
                            Generation.Entry<Integer, Long> entry = iterator.next();
                            assertTrue("entry: "+entry+" rand: "+rand, entry.getKey() >= rand);
                            assertTrue(entry.getKey().longValue() == entry.getValue());
                            if (insertionindex >= 0) {
                                assertTrue(rand == ints[insertionindex]);
                                assertTrue(entry.getKey() == rand);
                                Generation.Entry<Integer, Long> result = reader.get(rand);
                                assertTrue(result.getValue() == rand);
                            } else {
                                if (insertionindex != -1) assertTrue(ints[(~insertionindex)-1] < rand);
                                assertTrue("insertionindex: "+insertionindex+" entry: "+entry+" ints[!insertionindex]"+ints[~insertionindex], ints[~insertionindex] == entry.getKey());
                                Generation.Entry<Integer, Long> result = reader.get(rand);
                                assertTrue(result == null);
                            }
                        }
                    } finally {
                        done.decrementAndGet();
                    }
                }
            }).start();
        }
        while (done.get() > 0) {
            Thread.yield();
        }
        reader.close();
    }

    public void testGaps() throws Exception {
        int[] ints = createTree();
        ImmutableBTreeIndex.Reader<Integer, Long> reader = new ImmutableBTreeIndex.Reader(tmpDir, new IntSerializer(), new LongSerializer(), false);
        int index = 0;
        for (int i = 0; i <= ints[ints.length-1]; i++) {
            int current = ints[index];
            if (i > current) {
                index++;
                current = ints[index];
            }
            Generation.Entry<Integer, Long> result = reader.get(i);
            if (i < current) {
                assertTrue(result == null);
            } else {
                assertTrue(result.getValue() == i);
            }
            Iterator<Generation.Entry<Integer, Long>> iterator = reader.iterator(i, true);
            assertTrue(iterator.hasNext());
            Generation.Entry<Integer, Long> entry = iterator.next();
            assertTrue(entry.getKey() == current);
            assertTrue(entry.getValue() == current);
        }
    }

    public void testSeekPrevious() throws Exception {
        final int[] ints = createTree();
        final ImmutableBTreeIndex.Reader<Integer, Long> reader = new ImmutableBTreeIndex.Reader(tmpDir, new IntSerializer(), new LongSerializer(), false);
        final int max = ints[ints.length-1];
        final AtomicInteger done = new AtomicInteger(8);
        for (int i = 0; i < 8; i++) {
            final int index = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        final Random r = new Random(index);
                        for (int i = 0; i < treeSize; i++) {
                            int rand = r.nextInt(max+10);
                            int insertionindex = Arrays.binarySearch(ints, rand);
                            final Iterator<Generation.Entry<Integer, Long>> iterator = reader.reverseIterator(rand, true);
                            final boolean hasPrevious = iterator.hasNext();
                            Generation.Entry<Integer, Long> entry = null;
                            assertEquals("rand: "+rand+" hasPrevious: "+hasPrevious+(hasPrevious ? " previous: "+(entry = iterator.next()) : ""), hasPrevious, insertionindex != -1);
                            if (hasPrevious) {
                                if (entry == null) entry = iterator.next();
                                assertTrue(entry.getKey() <= rand);
                                assertTrue(entry.getKey().longValue() == entry.getValue());
                            }
                            if (insertionindex >= 0) {
                                if (entry == null) entry = iterator.next();
                                assertTrue(rand == ints[insertionindex]);
                                assertTrue(entry.getKey() == rand);
                                Generation.Entry<Integer, Long> result = reader.get(rand);
                                assertTrue(result.getValue() == rand);
                            } else {
                                if (hasPrevious) {
                                    assertTrue(ints[(~insertionindex)-1] < rand);
                                    assertTrue(ints[(~insertionindex)-1] == entry.getKey());
                                }
                                Generation.Entry<Integer, Long> result = reader.get(rand);
                                assertTrue(result == null);
                            }
                        }
                    } finally {
                        done.decrementAndGet();
                    }
                }
            }).start();
        }
        while (done.get() > 0) {
            Thread.yield();
        }
        reader.close();
    }

    public void testLargeKeys() throws IOException {


        final TreeMap<String, Long> map = Maps.newTreeMap();
        final Random r = new Random(0);
        final String[] strings = new String[10000];
        for (int i = 0; i < strings.length; i++) {
            final byte[] bytes = new byte[16384];
            r.nextBytes(bytes);
            strings[i] = new String(Base64.encodeBase64(bytes));
        }
        Arrays.sort(strings);
        Iterator<Generation.Entry<String, Long>> iterator = new AbstractIterator<Generation.Entry<String, Long>>() {
            int index = 0;
            @Override
            protected Generation.Entry<String, Long> computeNext() {
                if (index >= strings.length) return endOfData();
                final String s = strings[index];
                final long l = r.nextLong();
                index++;
                map.put(s, l);
                return Generation.Entry.create(s, l);
            }
        };
        ImmutableBTreeIndex.Writer.write(tmpDir, iterator, new StringSerializer(), new LongSerializer(), 65536, false);
        ImmutableBTreeIndex.Reader<String, Long> index = new ImmutableBTreeIndex.Reader<String, Long>(tmpDir, new StringSerializer(), new LongSerializer(), false);
        Iterator<Generation.Entry<String, Long>> it1 = index.iterator();
        Iterator<Map.Entry<String, Long>> it2 = map.entrySet().iterator();
        int i = 0;
        while (it2.hasNext()) {
            i++;
            assertTrue(it1.hasNext());
            Generation.Entry<String, Long> next1 = it1.next();
            Map.Entry<String, Long> next2 = it2.next();
            assertEquals(next1.getKey(), next2.getKey());
            assertEquals(next1.getValue(), next2.getValue());
        }
        assertFalse(it1.hasNext());
    }
}
