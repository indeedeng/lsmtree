package com.indeed.lsmtree.recordlog;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.indeed.util.io.BufferedFileDataOutputStream;
import com.indeed.util.io.UnsafeByteArrayOutputStream;
import com.indeed.util.serialization.Serializer;
import com.indeed.util.mmap.MMapBuffer;
import com.indeed.util.mmap.Memory;
import com.indeed.util.mmap.MemoryDataInput;
import fj.data.Option;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.log4j.Logger;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

/**
 * @author jplaisance
 */
public final class BasicRecordFile<E> implements RecordFile<E> {

    private static final byte[] CRC_SEED = Ints.toByteArray(0xC2A3066E);

    private static final Logger log = Logger.getLogger(BasicRecordFile.class);

    final MMapBuffer buffer;
    
    final Memory memory;

    private final File file;
    private final Serializer<E> serializer;


    public BasicRecordFile(File file, Serializer<E> serializer) throws IOException {
        this.file = file;
        this.serializer = serializer;
        buffer = new MMapBuffer(file, FileChannel.MapMode.READ_ONLY, ByteOrder.BIG_ENDIAN);
        memory = buffer.memory();
    }

    @Override
    public void close() throws IOException {
        buffer.close();
    }

    @Override
    public E get(long address) throws IOException {
        Option<E> option = readAndCheck(address, null);
        if (option.isNone()) throw new IOException("there is not a valid record at address "+address+" in file "+file.getAbsolutePath());
        return option.some();
    }

    @Override
    public RecordFile.Reader<E> reader() throws IOException {
        return new Reader();
    }

    @Override
    public RecordFile.Reader<E> reader(long address) throws IOException {
        return new Reader(address);
    }
    
    private Option<E> readAndCheck(long address, MutableLong nextElementStart) throws IOException {
        if (address+4 > memory.length()) {
            throw new ConsistencyException("not enough bytes in file");
        }
        final int length = memory.getInt(address);
        if (length < 0) {
            return Option.none();
        }
        if (address+8 > memory.length()) {
            throw new ConsistencyException("not enough bytes in file");
        }
        if (address+8+length > memory.length()) {
            throw new ConsistencyException("not enough bytes in file");
        }
        final int checksum = memory.getInt(address+4);
        MemoryDataInput in = new MemoryDataInput(memory);
        in.seek(address+8);
        CRC32 crc32 = new CRC32();
        crc32.update(CRC_SEED);
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        crc32.update(bytes);
        if ((int)crc32.getValue() != checksum) {
            throw new ConsistencyException("checksum for record does not match: expected "+checksum+" actual "+(int)crc32.getValue());
        }
        E ret = serializer.read(ByteStreams.newDataInput(bytes));
        if (nextElementStart != null) nextElementStart.setValue(address+8+length);
        return Option.some(ret);
    }

    private final class Reader implements RecordFile.Reader<E> {
        MutableLong position;
        E e;

        boolean done = false;

        private Reader() {
            this(0);
        }

        private Reader(long address) {
            position = new MutableLong(address);
        }

        @Override
        public boolean next() throws IOException {
            try {
                Option<E> option = readAndCheck(position.longValue(), position);
                if (option.isNone()) {
                    done = true;
                    return false;
                }
                e = option.some();
            } catch (ConsistencyException e) {
                done = true;
                log.warn("reading next record in "+file.getAbsolutePath()+" failed with exception", e);
                return false;
            }
            return true;
        }

        @Override
        public long getPosition() {
            return position.longValue();
        }

        @Override
        public E get() {
            return e;
        }

        @Override
        public void close() throws IOException {}
    }

    public static final class Writer<E> implements RecordFile.Writer<E> {

        final BufferedFileDataOutputStream out;
        private final Serializer<E> serializer;

        public Writer(File file, Serializer<E> serializer) throws FileNotFoundException {
            this.serializer = serializer;
            out = new BufferedFileDataOutputStream(file, ByteOrder.BIG_ENDIAN, 65536);
        }

        @Override
        public long append(final E e) throws IOException {
            UnsafeByteArrayOutputStream bytes = new UnsafeByteArrayOutputStream();
            serializer.write(e, new DataOutputStream(bytes));
            final long start = out.position();
            out.writeInt(bytes.size());
            final CRC32 checksum = new CRC32();
            checksum.update(CRC_SEED);
            checksum.update(bytes.getByteArray(), 0, bytes.size());
            out.writeInt((int)checksum.getValue());
            out.write(bytes.getByteArray(), 0, bytes.size());
            return start;
        }

        @Override
        public void close() throws IOException {
            out.writeInt(-1);
            out.sync();
            out.close();
        }

        public void sync() throws IOException {
            out.sync();
        }
    }
}
