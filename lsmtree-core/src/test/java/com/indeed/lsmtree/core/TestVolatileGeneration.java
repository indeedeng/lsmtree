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
import java.util.Random;

/**
* @author jplaisance
*/
public final class TestVolatileGeneration extends TestCase {

    private static final Logger log = Logger.getLogger(TestVolatileGeneration.class);

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

    public void testIterator() throws Exception {
        final File logPath = new File(tmpDir, "tmp.log");
        VolatileGeneration<Integer, Long> volatileGeneration = new VolatileGeneration(logPath, new IntSerializer(), new LongSerializer(), new ComparableComparator());
        int[] random = new int[1000000];
        Random r = new Random(0);
        for (int i = 0; i < random.length; i++) {
            random[i] = r.nextInt();
        }
        for (int element : random) {
            volatileGeneration.put(element, (long)element);
        }
        int[] sorted = new int[random.length];
        System.arraycopy(random, 0, sorted, 0, random.length);
        Arrays.sort(sorted);
        verifyIterationOrder(volatileGeneration, sorted);

        volatileGeneration.close();
        volatileGeneration = new VolatileGeneration<Integer, Long>(new File(tmpDir, "tmp2.log"), new IntSerializer(), new LongSerializer(), new ComparableComparator());
        volatileGeneration.replayTransactionLog(logPath);
        verifyIterationOrder(volatileGeneration, sorted);
    }

    private void verifyIterationOrder(final VolatileGeneration<Integer, Long> volatileGeneration, final int[] sorted) throws IOException {
        Iterator<Generation.Entry<Integer, Long>> iterator = volatileGeneration.iterator();
        for (int i = 0; i < sorted.length; i++) {
            while (i+1 < sorted.length && sorted[i] == sorted[i+1]) i++;
            assertTrue(iterator.hasNext());
            Generation.Entry<Integer, Long> next = iterator.next();
            assertTrue(next.getKey() == sorted[i]);
            assertTrue(next.getValue() == sorted[i]);
            assertFalse(next.isDeleted());
        }
        assertFalse(iterator.hasNext());
    }
}
