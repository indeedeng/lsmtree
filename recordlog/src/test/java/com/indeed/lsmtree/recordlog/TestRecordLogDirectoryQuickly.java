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

import com.indeed.util.compress.CompressionCodec;
import com.indeed.util.compress.SnappyCodec;
import com.indeed.util.serialization.StringSerializer;
import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * For brief RecordLogDirectory tests, as opposed to the longer-running ones in TestRecordLogDirectory.
 *
 * @Author: bsmith
 */
public class TestRecordLogDirectoryQuickly extends TestCase {
    private File dir;

    private int countBlocks(RecordFile.Reader reader) throws IOException {
        int blockCount = 0;
        while (reader.next()) {
            blockCount++;
        }

        return blockCount;
    }

    @Override
    public void setUp() throws Exception {
        dir = File.createTempFile("tmp","",new File("."));
        dir.delete();
        dir.mkdirs();

    }

    @Override
    public void tearDown() throws Exception {
        FileUtils.cleanDirectory(dir);
        FileUtils.deleteDirectory(dir);
    }

    public void testWithDeletedFirstFile() throws Exception {
        final com.indeed.util.serialization.Serializer serializer = new StringSerializer();
        final CompressionCodec codec = new SnappyCodec();
        RecordLogDirectory<String> recordLogDir =  new RecordLogDirectory.Builder(dir,serializer,codec).build();
        final RecordLogDirectory.Writer writer = RecordLogDirectory.Writer.create(dir, serializer, codec, 100);
        for (int i=0; i<10; i++) {
            writer.append(String.valueOf(i));
        }
        Thread.sleep(1000);
        for (int i=0 ;i<10; i++) {
            writer.append(String.valueOf(i));
        }
        writer.close();
        recordLogDir.close();

        recordLogDir =  new RecordLogDirectory.Builder(dir,serializer,codec).build();
        assertEquals(0, recordLogDir.getMinSegmentNum(dir));
        Assert.assertEquals(20, countBlocks(recordLogDir.reader()));

        final File firstFile = new File(dir,"000/000/000000000.rec");
        BlockCompressedRecordFile<String> bcrf = new BlockCompressedRecordFile.Builder(firstFile, serializer, codec)
                .setBlockSize(RecordLogDirectory.DEFAULT_BLOCK_SIZE)
                .setRecordIndexBits(RecordLogDirectory.DEFAULT_RECORD_INDEX_BITS)
                .setPadBits(RecordLogDirectory.DEFAULT_PAD_BITS)
                .build();
        final int firstFileBlockCount = countBlocks(bcrf.reader());

        Assert.assertTrue(firstFile.delete());

        final RecordLogDirectory<String> recordLogDir2 =  new RecordLogDirectory.Builder(dir,serializer,codec).build();
        Assert.assertEquals(20 - firstFileBlockCount, countBlocks(recordLogDir2.reader(recordLogDir2.getAddress(1))));
    }
}
