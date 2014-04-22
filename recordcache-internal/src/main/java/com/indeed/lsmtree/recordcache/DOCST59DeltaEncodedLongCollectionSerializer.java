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
 * Always interprets varints as signed 32-bit values. See DOCST-59, EVNT-115
 * @author dwahler
 */
public class DOCST59DeltaEncodedLongCollectionSerializer implements Serializer<Collection<Long>> {
    private static final Logger log = Logger.getLogger(DOCST59DeltaEncodedLongCollectionSerializer.class);

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
            // this double cast is important!
            // readVInt() is broken for values that don't fit in 5 bytes;
            // but even if values are larger than that, we want to truncate the result to a signed int.
            final long delta = (long) (int) VIntUtils.readVInt64(in);
            final long id = previous+delta;
            previous = id;
            ret.add(id);
        }
        return ret;
    }
}