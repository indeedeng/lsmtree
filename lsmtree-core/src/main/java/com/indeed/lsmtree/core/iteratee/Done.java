package com.indeed.lsmtree.core.iteratee;

import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class Done<A,B> implements Iteratee<A,B> {
    private static final Logger log = Logger.getLogger(Done.class);

    public final Input<A> remaining;
    
    public final B value;

    public Done(Input<A> remaining, B value) {
        this.remaining = remaining;
        this.value = value;
    }

    @Override
    public <Z> Z match(Matcher<A, B, Z> matcher) {
        return matcher.done(remaining, value);
    }
}
