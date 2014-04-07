package com.indeed.lsmtree.recordlog;

import com.google.common.base.Charsets;
import com.indeed.util.compress.CompressionCodec;
import com.indeed.util.compress.SnappyCodec;
import com.indeed.util.serialization.StringSerializer;
import com.indeed.util.serialization.array.ByteArraySerializer;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jplaisance
 */
public final class TestBlockCompressedRecordFile extends TestCase {

    private static final Logger log = Logger.getLogger(TestBlockCompressedRecordFile.class);

    File tmpDir;

    private ArrayList<String> strings;

    private ArrayList<Long> positions;

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

    public static CompressionCodec getCodec() {
        final SnappyCodec snappyCodec = new SnappyCodec();
        return snappyCodec;
    }

    public void testEmpty() throws Exception {
        final CompressionCodec codec = getCodec();
        final File testfile1 = new File(tmpDir, "testfile");
        BlockCompressedRecordFile.Writer<byte[]> writer = BlockCompressedRecordFile.Writer.open(testfile1, new ByteArraySerializer(), codec, 16384, 10, 6);
        writer.close();
        BlockCompressedRecordFile<byte[]> recordFile =
                new BlockCompressedRecordFile.Builder(testfile1, new ByteArraySerializer(), codec).build();
        RecordFile.Reader<byte[]> reader = recordFile.reader();
        assertFalse(reader.next());
        recordFile.close();
    }

    public void testSequential() throws IOException {
        BlockCompressedRecordFile<String> recordFile = createBlockCache();
        RecordFile.Reader<String> reader = recordFile.reader();
        int index = 0;
        while (reader.next()) {
            assertTrue(reader.get().equals(strings.get(index)));
            assertTrue(positions.get(index).equals(reader.getPosition()));
            index++;
        }
        assertTrue(index == strings.size());
        for (int i = 0; i < positions.size(); i++) {
            assertTrue(recordFile.get(positions.get(i)).equals(strings.get(i)));
        }
        reader.close();
        recordFile.close();
    }

    public void testRandom() throws IOException {
        final BlockCompressedRecordFile<String> recordFile = createBlockCache();
        final AtomicInteger done = new AtomicInteger(8);
        for (int i = 0; i < 8; i++) {
            final int index = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        final Random r = new Random(index);
                        for (int i = 0; i < 10000000; i++) {
                            int rand = r.nextInt(positions.size());
                            assertTrue(recordFile.get(positions.get(rand)).equals(strings.get(rand)));
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
        recordFile.close();
    }

    public void testRandomWithReader() throws IOException {
        final BlockCompressedRecordFile<String> recordFile = createBlockCache();
        final AtomicInteger done = new AtomicInteger(8);
        for (int i = 0; i < 8; i++) {
            final int index = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        final Random r = new Random(index);
                        for (int i = 0; i < 10000000; i++) {
                            int rand = r.nextInt(positions.size());
                            final RecordFile.Reader<String> reader = recordFile.reader(positions.get(rand));
                            assertTrue(reader.next());
                            assertEquals(reader.get(), strings.get(rand));
                            reader.close();
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
        recordFile.close();
    }

    private BlockCompressedRecordFile<String> createBlockCache() throws IOException {
        final CompressionCodec codec = getCodec();
        final File testfile1 = new File(tmpDir, "testfile");
        final BlockCompressedRecordFile.Writer<String> writer;
        writer = BlockCompressedRecordFile.Writer.open(testfile1, new StringSerializer(), codec, 16384, 10, 6);
        BufferedReader
                in = new BufferedReader(new InputStreamReader(TestBlockCompressedRecordFile.class.getClassLoader().getResourceAsStream("testinput.txt"), Charsets.UTF_8));
        strings = new ArrayList<String>();
        positions = new ArrayList<Long>();
        for (String line; (line = in.readLine()) != null;) {
            positions.add(writer.append(line));
            strings.add(line);
        }
        writer.close();
        return new BlockCompressedRecordFile.Builder(testfile1, new StringSerializer(), codec).build();
    }
}
