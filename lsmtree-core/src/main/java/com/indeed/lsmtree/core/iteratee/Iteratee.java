package com.indeed.lsmtree.core.iteratee;

import fj.F;

/**
 * @author jplaisance
 */
public interface Iteratee<A,B> {

    public <Z> Z match(Matcher<A,B,Z> matcher);

    public static abstract class Matcher<A,B,Z> {
        public Z done(Input<A> remaining, B value) {
            return otherwise();
        }
        
        public Z cont(F<Input<A>, Iteratee<A,B>> cont) {
            return otherwise();
        }
        
        public Z otherwise() {
            throw new UnsupportedOperationException();
        }
    }
}
