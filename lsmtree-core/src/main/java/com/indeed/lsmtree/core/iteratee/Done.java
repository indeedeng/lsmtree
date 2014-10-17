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

import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class Done<A,B> implements Iteratee<A,B> {
    private static final Logger log = Logger.getLogger(Done.class);

    public final Input<A> remaining;
    
    public final B value;

    public Done(Input<A> remaining, B value) {
        this.remaining = remaining;
        this.value = value;
    }

    @Override
    public <Z> Z match(Matcher<A, B, Z> matcher) {
        return matcher.done(remaining, value);
    }
}
