package com.indeed.lsmtree.core.iteratee;

import fj.F;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class Cont<A,B> implements Iteratee<A,B> {
    private static final Logger log = Logger.getLogger(Cont.class);
    
    public final F<Input<A>, Iteratee<A,B>> cont;

    public Cont(F<Input<A>, Iteratee<A, B>> cont) {
        this.cont = cont;
    }

    @Override
    public <Z> Z match(Matcher<A, B, Z> matcher) {
        return matcher.cont(cont);
    }
}
