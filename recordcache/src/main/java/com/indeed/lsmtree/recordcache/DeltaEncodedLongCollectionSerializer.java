package com.indeed.lsmtree.recordcache;

import com.indeed.util.io.VIntUtils;
import com.indeed.util.serialization.Serializer;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

/**
 * @author Julie Scully (julie@indeed.com)
 */
public class DeltaEncodedLongCollectionSerializer implements Serializer<Collection<Long>> {
    private static final Logger log = Logger.getLogger(DeltaEncodedLongCollectionSerializer.class);

    @Override
    public void write(final Collection<Long> longs, final DataOutput out) throws IOException {
        out.writeInt(longs.size());
        long previous = 0;
        for (long i : longs) {
            final long delta = i-previous;
            VIntUtils.writeVInt64(out, delta);
            previous = i;
        }
    }

    @Override
    public Collection<Long> read(final DataInput in) throws IOException {
        final int length = in.readInt();
        LongArrayList ret = new LongArrayList(length);
        long previous = 0;
        for (int i = 0; i < length; i++) {
            final long delta = VIntUtils.readVInt64(in);
            final long id = previous+delta;
            previous = id;
            ret.add(id);
        }
        return ret;
    }
}
