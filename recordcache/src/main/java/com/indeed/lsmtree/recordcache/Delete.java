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
 package com.indeed.lsmtree.recordcache;

import org.apache.log4j.Logger;

import java.util.Collection;

/**
 * @author jplaisance
 */
public final class Delete<K> implements Operation {

    private static final Logger log = Logger.getLogger(Delete.class);

    private final Collection<K> keys;

    public Delete(Collection<K> keys) {
        this.keys = keys;
    }

    public Collection<K> getKeys() {
        return keys;
    }
}
