package com.indeed.lsmtree.recordlog;

import com.google.common.base.Charsets;
import com.indeed.util.compress.CompressionCodec;
import com.indeed.util.compress.SnappyCodec;
import com.indeed.util.serialization.StringSerializer;
import fj.data.Option;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jplaisance
 */
public final class TestRecordLogDirectory extends TestCase {

    private static final Logger log = Logger.getLogger(TestRecordLogDirectory.class);

    private static final int FILE_INDEX_BITS = 28;

    private static final int RECORD_INDEX_BITS = 10;

    private static final int PAD_BITS = 6;

    private static final int BLOCK_SIZE = 16384;

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
        RecordLogDirectory.Writer writer = RecordLogDirectory.Writer.create(tmpDir, new StringSerializer(), codec, Integer.MAX_VALUE, BLOCK_SIZE, FILE_INDEX_BITS,
                RECORD_INDEX_BITS, PAD_BITS
        );
        writer.roll();
        writer.close();
        RecordLogDirectory recordLogDirectory = new RecordLogDirectory(tmpDir, new StringSerializer(), 8192, codec, BLOCK_SIZE, FILE_INDEX_BITS,
                RECORD_INDEX_BITS, PAD_BITS);
        RecordFile.Reader reader = recordLogDirectory.reader();
        assertFalse(reader.next());
        reader.close();
        recordLogDirectory.close();
    }

    public void testSequential() throws Exception {
        RecordLogDirectory<String> recordLogDirectory = createRecordLogDirectory();
        RecordFile.Reader<String> reader = recordLogDirectory.reader();
        int index = 0;
        while (reader.next()) {
            assertTrue(reader.get().equals(strings.get(index)));
            assertTrue(reader.getPosition() == positions.get(index));
            index++;
        }
        assertTrue(index+" != "+ strings.size(), index == strings.size());
        reader.close();
        Option<RecordFile.Reader<String>> option = recordLogDirectory.getFileReader(0);
        assertTrue(option.isSome());
        reader = option.some();
        index = 0;
        while (reader.next()) {
            assertTrue(reader.get().equals(strings.get(index)));
            assertTrue(reader.getPosition() == positions.get(index));
            index++;
        }
        reader.close();
        for (int i = 0; i < positions.size(); i++) {
            assertTrue(recordLogDirectory.get(positions.get(i)).equals(strings.get(i)));
        }

        recordLogDirectory.close();
    }

    public void testRandom() throws Exception {
        final RecordLogDirectory<String> fileCache = createRecordLogDirectory();
        final AtomicInteger done = new AtomicInteger(8);
        for (int i = 0; i < 8; i++) {
            final int index = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        final Random r = new Random(index);
                        for (int i = 0; i < 10000; i++) {
                            int rand = r.nextInt(positions.size());
                            assertTrue(fileCache.get(positions.get(rand)).equals(strings.get(rand)));
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
        fileCache.close();
    }

    public void testRandomWithReader() throws Exception {
        final RecordLogDirectory<String> fileCache = createRecordLogDirectory();
        final AtomicInteger done = new AtomicInteger(8);
        for (int i = 0; i < 8; i++) {
            final int index = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        final Random r = new Random(index);
                        for (int i = 0; i < 10000; i++) {
                            int rand = r.nextInt(positions.size());
                            final RecordFile.Reader<String> reader = fileCache.reader(positions.get(rand));
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
        fileCache.close();
    }

    private RecordLogDirectory<String> createRecordLogDirectory() throws IOException {
        final CompressionCodec codec = getCodec();
        RecordLogDirectory.Writer<String> writer = RecordLogDirectory.Writer.create(tmpDir, new StringSerializer(), codec, Integer.MAX_VALUE,
                BLOCK_SIZE,
                FILE_INDEX_BITS, RECORD_INDEX_BITS, PAD_BITS
        );
        BufferedReader
                in = new BufferedReader(new InputStreamReader(new FileInputStream("src/test/resources/enwiki-latest-stub-articles4.xml"), Charsets.UTF_8), 65536);
        strings = new ArrayList<String>();
        positions = new ArrayList<Long>();
        outer: while (true) {
            for (int i = 0; i < 1000; i++) {
                String line = in.readLine();
                if (line == null) break outer;
                positions.add(writer.append(line));
                strings.add(line);
            }
            writer.roll();
        }
        writer.close();
        writer.verifySegmentIntegrity(tmpDir, 0);
        return new RecordLogDirectory(tmpDir, new StringSerializer(), 8192, codec, BLOCK_SIZE, FILE_INDEX_BITS, RECORD_INDEX_BITS, PAD_BITS);
    }
}
