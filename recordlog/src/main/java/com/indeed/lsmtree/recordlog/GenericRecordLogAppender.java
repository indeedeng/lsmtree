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

    public GenericRecordLogAppender(File file, Serializer<T> serializer, CompressionCodec codec, AtomicReference<Map<String, String>> metadataRef) throws IOException {
        this(file, serializer, codec, metadataRef, Long.MAX_VALUE);
    }

    public GenericRecordLogAppender(File file, Serializer<T> serializer, CompressionCodec codec, AtomicReference<Map<String, String>> metadataRef,
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

    public File getSegmentPath(int segment) {
        return RecordLogDirectory.getSegmentPath(file, segment);
    }

    protected long writeOperation(final T op) throws IOException {
        lastPosition = writer.append(op);
        return lastPosition;
    }

    public synchronized void flushWriter(Map<String, String> metadata) throws IOException {
        writer.roll();
        maxSegment = (int)(lastPosition >>> (64-RecordLogDirectory.DEFAULT_FILE_INDEX_BITS));
        metadata.put(LAST_POSITION_KEY, String.valueOf(lastPosition));
        metadata.put(MAX_SEGMENT_KEY, String.valueOf(maxSegment));
        writeStringToFile(metadataPath, mapper.writeValueAsString(metadata));
        writeStringToFile(lastPositionPath, String.valueOf(lastPosition));
        writeStringToFile(maxSegmentPath, String.valueOf(maxSegment));
    }

    public static long readLongFromFile(File f, long defaultVal) throws IOException {
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

    public static void writeStringToFile(File f, String str) throws IOException {
        File nextPath = new File(f.getParentFile(), f.getName()+".next");
        BufferedFileDataOutputStream out = new BufferedFileDataOutputStream(nextPath);
        out.write(str.getBytes(Charsets.UTF_8));
        out.sync();
        out.close();
        nextPath.renameTo(f);
    }
}
