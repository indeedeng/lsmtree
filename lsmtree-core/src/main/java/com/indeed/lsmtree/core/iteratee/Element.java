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
