/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
