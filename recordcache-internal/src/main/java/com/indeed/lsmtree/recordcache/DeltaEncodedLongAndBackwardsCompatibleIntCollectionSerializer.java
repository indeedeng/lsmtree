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
 * This class is specific for use in the docstore.  If you're reading this to determine if this class is for you, the
 * answer is almost certainly no.
 *
 * Legacy jobid collections were written unsorted and packed as deltas, so there were negative values.  Now that jobids
 * will be long, we will pack long values as tightly as we can, so the leading bit will be set when there is a long jobid.
 * It is ambiguous whether a leading bit set means it's a negative int or a long so they cannot both be deserialized in
 * the same manner.
 *
 * To get around this, we will start setting the leading bit of the length of the collection when we write.  This will
 * indicate that the list should be deserialized as sorted longs.  If the leading bit is not set, then it was written by
 * the legacy serializer and should be decoded as potentially unsorted ints.
 *
 * See notes in DOCST-59 and DOCST-72.
 *
 * @author Julie Scully (julie@indeed.com)
 */
public class DeltaEncodedLongAndBackwardsCompatibleIntCollectionSerializer implements Serializer<Collection<Long>> {
    private static final Logger log = Logger.getLogger(DeltaEncodedLongCollectionSerializer.class);

    @Override
    public void write(final Collection<Long> longs, final DataOutput out) throws IOException {
        final int size = longs.size();
        final int sizeWithSetLeadingBit = size | 0x80000000;
        out.writeInt(sizeWithSetLeadingBit);
        long previous = 0;
        for (long i : longs) {
            final long delta = i-previous;
            VIntUtils.writeVInt64(out, delta);
            previous = i;
        }
    }

    @Override
    public Collection<Long> read(final DataInput in) throws IOException {
        final int rawLength = in.readInt();
        if (rawLength < 0) {
            // This was written as longs.
            return readLongs(in, rawLength & 0x7FFFFFFF);
        } else {
            // This was written as ints.
            return readInts(in, rawLength);
        }
    }

    private Collection<Long> readLongs(final DataInput in, final int length) throws IOException {
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

    private Collection<Long> readInts(final DataInput in, final int length) throws IOException {
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
