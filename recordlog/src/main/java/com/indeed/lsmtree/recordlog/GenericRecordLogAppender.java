package com.indeed.lsmtree.recordlog;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.indeed.util.compress.CompressionCodec;
import com.indeed.util.io.BufferedFileDataOutputStream;
import com.indeed.util.serialization.Serializer;
import com.indeed.util.varexport.Export;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.joda.time.DateTime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jplaisance
 */
public class GenericRecordLogAppender<T> {
    private static final Logger log = Logger.getLogger(GenericRecordLogAppender.class);

    public static final String LAST_POSITION_KEY = "lastposition";

    public static final String MAX_SEGMENT_KEY = "maxsegment";

    private long lastPosition;

    private int maxSegment;

    private final RecordLogDirectory.Writer<T> writer;

    private final File lastPositionPath;

    private final File maxSegmentPath;

    private final File metadataPath;

    private final File file;

    private final ObjectMapper mapper;

    public GenericRecordLogAppender(File file,
                                    Serializer<T> serializer,
                                    CompressionCodec codec,
                                    AtomicReference<Map<String, String>> metadataRef) throws IOException {
        this(file, serializer, codec, metadataRef, Long.MAX_VALUE);
    }

    /**
     * @param file              root directory for record logs
     * @param serializer        serializer
     * @param codec             compression codec
     * @param metadataRef       if non null, this will contain a reference to metadata after construction if it existed
     * @param rollFrequency     how frequently new record files should be created in milliseconds, but only if there
     *                          is a new write. If set to MAX_LONG, new record files will only be created when
     *                          {@link #flushWriter(java.util.Map)} is called
     *
     * @throws IOException      if an I/O error occurs
     */
    public GenericRecordLogAppender(@Nonnull File file,
                                    @Nonnull Serializer<T> serializer,
                                    @Nonnull CompressionCodec codec,
                                    @Nullable AtomicReference<Map<String, String>> metadataRef,
                                    long rollFrequency) throws IOException {
        mapper = new ObjectMapper();
        mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
        this.file = file;
        lastPositionPath = new File(this.file, "lastposition.txt");
        maxSegmentPath = new File(file, "maxsegment.txt");
        metadataPath = new File(file, "metadata.json");
        if (metadataPath.exists()) {
            Map<String, String> metadata = readMetadata(metadataPath, mapper);
            if (metadataRef != null) metadataRef.set(metadata);
            lastPosition = Long.parseLong(metadata.get(LAST_POSITION_KEY));
            maxSegment = Integer.parseInt(metadata.get(MAX_SEGMENT_KEY));
            log.info("lastposition: "+lastPosition);
            log.info("maxsegment: "+maxSegment);
        } else {
            lastPosition = readLongFromFile(lastPositionPath, 0);
            maxSegment = -1;
        }
        writer = maxSegment < 0 ?
                RecordLogDirectory.Writer.create(file, serializer, codec, rollFrequency) :
                RecordLogDirectory.Writer.create(file, serializer, codec, rollFrequency, maxSegment);
    }

    @Export(name = "last-position", doc = "Last address that was written to")
    public long getLastPosition() {
        return lastPosition;
    }

    @Export(name = "max-segment", doc = "Segment number that the last position is currently writing to")
    public int getMaxSegment() {
        return maxSegment;
    }

    @Export(name="max-segment-timestamp", doc = "Timestamp of max segment written to record log directory")
    public long getMaxSegmentTimestamp() {
        return getSegmentPath(getMaxSegment()).lastModified();
    }

    @Export(name="max-segment-timestring", doc = "Same as max-segment-timestamp but just in human readable form")
    public String getMaxSegmentTimestring() {
        return new DateTime(getMaxSegmentTimestamp()).toString();
    }

    private static Map<String, String> readMetadata(File metadataPath, ObjectMapper mapper) throws IOException {
        if (metadataPath.exists()) {
            Map<String, String> ret = Maps.newLinkedHashMap();
            JsonNode node = mapper.readTree(Files.toString(metadataPath, Charsets.UTF_8));
            Iterator<Map.Entry<String,JsonNode>> iterator = node.getFields();
            while (iterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = iterator.next();
                ret.put(entry.getKey(), entry.getValue().getTextValue());
            }
            return ret;
        }
        return null;
    }

    /**
     * @param segment   segment number
     * @return          a file pointing at the segment
     */
    public File getSegmentPath(int segment) {
        return RecordLogDirectory.getSegmentPath(file, segment);
    }

    /**
     * Writes an entry to the record file. This write is not guaranteed to be persisted
     * unless {@link #flushWriter(java.util.Map)} is called.
     *
     * @param op            operation to write
     * @return              address operation was written to
     * @throws IOException  if an I/O error occurs
     */
    protected long writeOperation(final T op) throws IOException {
        lastPosition = writer.append(op);
        return lastPosition;
    }

    /**
     * Creates a new segment file and flushes buffered writes to disk.
     * Provided metadata is written to record_log_directory_root/metadata.json
     *
     * @param metadata       a mutable map of metadata to be written, may be empty.
     * @throws IOException   if an I/O error occurs
     */
    public synchronized void flushWriter(@Nonnull Map<String, String> metadata) throws IOException {
        writer.roll();
        maxSegment = (int)(lastPosition >>> (64-RecordLogDirectory.DEFAULT_FILE_INDEX_BITS));
        metadata.put(LAST_POSITION_KEY, String.valueOf(lastPosition));
        metadata.put(MAX_SEGMENT_KEY, String.valueOf(maxSegment));
        writeStringToFile(metadataPath, mapper.writeValueAsString(metadata));
        writeStringToFile(lastPositionPath, String.valueOf(lastPosition));
        writeStringToFile(maxSegmentPath, String.valueOf(maxSegment));
    }

    /**
     * @param f             file that stores a single long as a UTF-8 string
     * @param defaultVal    value to return if file does not exist
     * @return              long value stored in file, or default value if it does not exist
     * @throws IOException  if an I/O error occurs
     */
    public static long readLongFromFile(@Nonnull File f, long defaultVal) throws IOException {
        if (!f.exists()) {
            return defaultVal;
        } else {
            RandomAccessFile in = new RandomAccessFile(f, "r");
            int length = (int)in.length();
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            in.close();
            return Long.parseLong(new String(bytes, Charsets.UTF_8).trim());
        }
    }

    /**
     * Writes a string to a file, overwriting it if it exists.
     *
     * @param f             file to write
     * @param str           value to store
     * @throws IOException  if an I/O error occurs
     */
    public static void writeStringToFile(File f, String str) throws IOException {
        File nextPath = new File(f.getParentFile(), f.getName()+".next");
        BufferedFileDataOutputStream out = new BufferedFileDataOutputStream(nextPath);
        out.write(str.getBytes(Charsets.UTF_8));
        out.sync();
        out.close();
        nextPath.renameTo(f);
    }
}
