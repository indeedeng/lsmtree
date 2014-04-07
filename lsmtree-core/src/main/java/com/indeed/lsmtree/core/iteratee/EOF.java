package com.indeed.lsmtree.core.iteratee;

import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class EOF<E> implements Input<E> {
    private static final Logger log = Logger.getLogger(EOF.class);

    public static <E> EOF<E> eof() {
        return eof;
    }

    private static final EOF eof = new EOF();

    private EOF() {}

    @Override
    public <Z> Z match(Matcher<E, Z> matcher) {
        return matcher.eof();
    }
}
