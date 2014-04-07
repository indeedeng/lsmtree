package com.indeed.lsmtree.core.iteratee;

/**
 * @author jplaisance
 */
public interface Input<E> {
    
    public <Z> Z match(Matcher<E,Z> matcher);

    public static abstract class Matcher<E,Z> {
        public Z eof() {
            return otherwise();
        }

        public Z empty() {
            return otherwise();
        }

        public Z element(E e) {
            return otherwise();
        }

        public Z otherwise() {
            throw new UnsupportedOperationException();
        }
    }
}
