package com.indeed.lsmtree.core.iteratee;

import fj.F;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public abstract class Processor<A,B> {
    private static final Logger log = Logger.getLogger(Processor.class);

    public abstract Iteratee<A,B> process(Input<A> input);
    
    private final F<Input<A>, Iteratee<A,B>> process = new F<Input<A>, Iteratee<A, B>>() {
        @Override
        public Iteratee<A, B> f(Input<A> a) {
            return process(a);
        }
    };
    
    private final Cont<A,B> cont = new Cont<A, B>(process);

    protected final EOF<A> EOF() {
        return EOF.eof();
    }
    
    protected final Empty<A> Empty() {
        return Empty.empty();
    }
    
    protected final Element<A> Element(A a) {
        return Element.element(a);
    }

    public final Cont<A,B> Cont() {
        return cont;
    }
    
    protected final Done<A,B> Done(B value) {
        return Done(Empty(), value);
    }

    protected final Done<A,B> Done(Input<A> remaining, B value) {
        return new Done<A, B>(remaining, value);
    }
}
