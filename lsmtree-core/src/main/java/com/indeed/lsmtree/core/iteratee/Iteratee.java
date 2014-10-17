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
