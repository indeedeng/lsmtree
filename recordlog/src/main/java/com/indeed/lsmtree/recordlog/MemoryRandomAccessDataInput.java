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
