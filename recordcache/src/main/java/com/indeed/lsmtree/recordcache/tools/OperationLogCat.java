package com.indeed.lsmtree.recordcache.tools;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.util.compress.CompressionCodec;
import com.indeed.lsmtree.recordcache.Checkpoint;
import com.indeed.lsmtree.recordcache.Delete;
import com.indeed.lsmtree.recordcache.Operation;
import com.indeed.lsmtree.recordcache.OperationSerializer;
import com.indeed.lsmtree.recordcache.Put;
import com.indeed.lsmtree.recordlog.BlockCompressedRecordFile;
import com.indeed.lsmtree.recordlog.RecordFile;
import com.indeed.util.serialization.Serializer;
import com.indeed.util.serialization.Stringifier;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author jplaisance
 */
public final class OperationLogCat {

    private static final Logger log = Logger.getLogger(OperationLogCat.class);

    public static <K,V> void cat(
            File file,
            CompressionCodec codec,
            Serializer<K> keySerizlizer,
            Serializer<V> valueSerializer,
            Serializer<Collection<K>> keyCollectionSerializer,
            Stringifier<K> keyStringifier,
            Stringifier<V> valueStringifier
    ) throws IOException {
        OperationSerializer<K, V> serializer = new OperationSerializer<K, V>(keySerizlizer, valueSerializer, keyCollectionSerializer);
        BlockCompressedRecordFile<Operation> recordFile =
                new BlockCompressedRecordFile.Builder(file, serializer, codec).build();
        RecordFile.Reader<Operation> reader = recordFile.reader();

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> map = Maps.newLinkedHashMap();
        List<K> keys = Lists.newArrayList();
        while (reader.next()) {
            map.clear();
            keys.clear();
            Operation op = reader.get();
            map.put("position", String.valueOf(reader.getPosition()));
            map.put("type", op.getClass().getSimpleName());
            if (op.getClass() == Put.class) {
                Put<K,V> put = (Put<K, V>)op;
                map.put("key", keyStringifier.toString(put.getKey()));
                map.put("value", valueStringifier.toString(put.getValue()));
            } else if (op.getClass() == Delete.class) {
                Delete<K> delete = (Delete<K>)op;
                for (K key : delete.getKeys()) {
                    keys.add(key);
                }
                map.put("keys", keys);
            } else if (op.getClass() == Checkpoint.class) {
                Checkpoint checkpoint = (Checkpoint)op;
                map.put("timestamp", checkpoint.getTimestamp());
            }
            System.out.println(mapper.writeValueAsString(map));
        }
    }
}
