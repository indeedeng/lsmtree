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
import fj.P;
import fj.P2;
import fj.data.Stream;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class Enumerator {
    private static final Logger log = Logger.getLogger(Enumerator.class);

    public static <A,B> P2<B, Stream<A>> runOnce(Processor<A,B> processor, final Stream<A> stream) {
        Cont<A,B> cont = processor.Cont();
        return new RunOnce<A,B>(cont, stream).run();
    }
    
    private static final class RunOnce<A,B> {
        Iteratee<A,B> it;
        Stream<A> stream;
        P2<B,Stream<A>> ret;

        private RunOnce(Iteratee<A, B> it, Stream<A> stream) {
            this.it = it;
            this.stream = stream;
        }

        P2<B,Stream<A>> run() {
            Iteratee.Matcher<A,B,Boolean> matcher = new Iteratee.Matcher<A,B,Boolean>() {
                @Override
                public Boolean done(Input<A> remaining, final B value) {
                    ret = remaining.match(new Input.Matcher<A, P2<B, Stream<A>>>() {

                        public P2<B, Stream<A>> eof() {
                            return P.p(value, Stream.<A>nil());
                        }

                        public P2<B, Stream<A>> empty() {
                            return P.p(value, stream);
                        }

                        public P2<B, Stream<A>> element(A a) {
                            return P.p(value, stream.cons(a));
                        }
                    });
                    return true;
                }

                @Override
                public Boolean cont(F<Input<A>, Iteratee<A, B>> cont) {
                    if (stream == null) {
                        throw new IllegalStateException("stream cannot be null, to avoid this ensure that EOF forces Done to be returned");
                    } if (stream.isEmpty()) {
                        it = cont.f(EOF.<A>eof());
                        stream = null;
                        return false;
                    } else {
                        it = cont.f(Element.element(stream.head()));
                        stream = stream.tail()._1();
                        return false;
                    }
                }
            };
            while (!it.match(matcher));
            return ret;
        }
    }
}
