package com.indeed.lsmtree.core.iteratee;

/**
 * @author jplaisance
 */
public final class Element<E> implements Input<E> {
    
    public final E e;
    
    public static <E> Element<E> element(E e) {
        return new Element<E>(e);
    }

    private Element(E e) {
        this.e = e;
    }

    @Override
    public <Z> Z match(Matcher<E, Z> matcher) {
        return matcher.element(e);
    }
}
