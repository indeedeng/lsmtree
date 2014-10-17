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

import com.indeed.util.io.RandomAccessDataInput;
import com.indeed.util.mmap.Memory;
import com.indeed.util.mmap.MemoryDataInput;
import org.apache.log4j.Logger;

import javax.annotation.WillNotClose;
import java.io.IOException;

/**
 * @author jplaisance
 */
public final class MemoryRandomAccessDataInput implements RandomAccessDataInput {
    private static final Logger log = Logger.getLogger(MemoryRandomAccessDataInput.class);

    private final MemoryDataInput dataInput;

    public MemoryRandomAccessDataInput(final @WillNotClose Memory memory) {
        dataInput = new MemoryDataInput(memory);
    }

    @Override
    public void readFully(final byte[] b) throws IOException {
        dataInput.readFully(b);
    }

    @Override
    public void readFully(final byte[] b, final int off, final int len) throws IOException {
        dataInput.readFully(b, off, len);
    }

    @Override
    public int skipBytes(final int n) throws IOException {
        return dataInput.skipBytes(n);
    }

    @Override
    public boolean readBoolean() throws IOException {
        return dataInput.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return dataInput.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return dataInput.readUnsignedByte();
    }

    @Override
    public short readShort() throws IOException {
        return dataInput.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return dataInput.readUnsignedShort();
    }

    @Override
    public char readChar() throws IOException {
        return dataInput.readChar();
    }

    @Override
    public int readInt() throws IOException {
        return dataInput.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return dataInput.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        return dataInput.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        return dataInput.readDouble();
    }

    @Override
    public String readLine() throws IOException {
        return dataInput.readLine();
    }

    @Override
    public String readUTF() throws IOException {
        return dataInput.readUTF();
    }

    public void seek(final long position) {
        dataInput.seek(position);
    }

    public long position() {
        return dataInput.position();
    }

    public long length() {
        return dataInput.length();
    }

    public void close() throws IOException {}
}
