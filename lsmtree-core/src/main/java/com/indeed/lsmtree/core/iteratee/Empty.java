package com.indeed.lsmtree.core.iteratee;

import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class Empty<E> implements Input<E> {
    private static final Logger log = Logger.getLogger(Empty.class);
    
    public static <E> Empty<E> empty() {
        return empty;
    }
    
    private static final Empty empty = new Empty();

    private Empty() {}

    @Override
    public <Z> Z match(Matcher<E, Z> matcher) {
        return matcher.empty();
    }
}
