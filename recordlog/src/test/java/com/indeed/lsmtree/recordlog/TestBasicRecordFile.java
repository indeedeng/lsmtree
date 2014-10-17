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

import com.indeed.util.serialization.array.ByteArraySerializer;
import junit.framework.TestCase;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * @author jplaisance
 */
public final class TestBasicRecordFile extends TestCase {

    private static final Logger log = Logger.getLogger(TestBasicRecordFile.class);

    public void testNormalOperation() throws IOException {
        File temp = File.createTempFile("basicrecordfile", ".rec");
        long[] addresses = writeFile(temp, true);
        readFile(temp, addresses);
    }

    private void readFile(final File temp, final long[] addresses) throws IOException {
        Random rand = new Random(0);
        BasicRecordFile<byte[]> recordFile = new BasicRecordFile<byte[]>(temp, new ByteArraySerializer());
        RecordFile.Reader<byte[]> reader = recordFile.reader();
        int i = 0;
        try {
            for (i = 0; i < 100000; i++) {
                byte[] bytes = new byte[rand.nextInt(100)];
                for (int j = 0; j < bytes.length; j++) {
                    bytes[j] = (byte)rand.nextInt();
                }
                assertTrue("read failed on iteration "+i, reader.next());
                assertTrue(Arrays.equals(reader.get(), bytes));
            }
            assertFalse(reader.next());
        } catch (IOException e) {
            log.error("read failed on iteration "+i, e);
            throw e;
        }
        rand = new Random(0);
        try {
            for (i = 0; i < 100000; i++) {
                byte[] bytes = new byte[rand.nextInt(100)];
                for (int j = 0; j < bytes.length; j++) {
                    bytes[j] = (byte)rand.nextInt();
                }
                assertTrue(Arrays.equals(recordFile.get(addresses[i]), bytes));
            }
        } catch (IOException e) {
            log.error("read failed on iteration "+i, e);
            throw e;
        }
        reader.close();
        recordFile.close();
    }

    private long[] writeFile(final File temp, boolean close) throws IOException {
        Random rand = new Random(0);
        BasicRecordFile.Writer<byte[]> writer = new BasicRecordFile.Writer<byte[]>(temp, new ByteArraySerializer());
        long[] addresses = new long[100000];
        for (int i = 0; i < 100000; i++) {
            byte[] bytes = new byte[rand.nextInt(100)];
            for (int j = 0; j < bytes.length; j++) {
                bytes[j] = (byte)rand.nextInt();
            }
            addresses[i] = writer.append(bytes);
        }
        if (close) writer.close();
        else writer.sync();
        return addresses;
    }

    public void testNotClosedWriter() throws IOException, InterruptedException {
        File temp = File.createTempFile("basicrecordfile", ".rec");
        long[] addresses = writeFile(temp, false);
        readFile(temp, addresses);
        Thread.sleep(1000);
    }
}
