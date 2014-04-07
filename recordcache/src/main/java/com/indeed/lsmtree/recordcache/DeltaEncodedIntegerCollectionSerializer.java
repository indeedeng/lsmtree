package com.indeed.lsmtree.recordcache;

import com.indeed.util.io.VIntUtils;
import com.indeed.util.serialization.Serializer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

/**
 * @author jplaisance
 */
public final class DeltaEncodedIntegerCollectionSerializer implements Serializer<Collection<Integer>> {

    private static final Logger log = Logger.getLogger(DeltaEncodedIntegerCollectionSerializer.class);

    @Override
    public void write(final Collection<Integer> integers, final DataOutput out) throws IOException {
        out.writeInt(integers.size());
        int previous = 0;
        for (int i : integers) {
            final int delta = i-previous;
            VIntUtils.writeVInt(out, delta);
            previous = i;
        }
    }

    @Override
    public Collection<Integer> read(final DataInput in) throws IOException {
        final int length = in.readInt();
        IntArrayList ret = new IntArrayList(length);
        int previous = 0;
        for (int i = 0; i < length; i++) {
            final int delta = VIntUtils.readVInt(in);
            final int id = previous+delta;
            previous = id;
            ret.add(id);
        }
        return ret;
    }
}
