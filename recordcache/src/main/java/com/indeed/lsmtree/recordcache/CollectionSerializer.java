package com.indeed.lsmtree.recordcache;

import com.indeed.util.serialization.Serializer;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author jplaisance
 */
public final class CollectionSerializer<K> implements Serializer<Collection<K>> {

    private static final Logger log = Logger.getLogger(CollectionSerializer.class);

    private final Serializer<K> serializer;

    public CollectionSerializer(Serializer<K> serializer) {
        this.serializer = serializer;
    }

    @Override
    public void write(final Collection<K> ks, final DataOutput out) throws IOException {
        out.writeInt(ks.size());
        for (K k : ks) {
            serializer.write(k, out);
        }
    }

    @Override
    public Collection<K> read(final DataInput in) throws IOException {
        int length = in.readInt();
        ArrayList<K> ret = new ArrayList<K>(length);
        for (int i = 0; i < length; i++) {
            ret.add(serializer.read(in));
        }
        return ret;
    }
}
