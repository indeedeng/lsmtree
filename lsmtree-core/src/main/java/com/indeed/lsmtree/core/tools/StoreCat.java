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
 package com.indeed.lsmtree.core.tools;

import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.indeed.util.io.MD5OutputStream;
import com.indeed.util.serialization.Serializer;
import com.indeed.util.serialization.Stringifier;
import com.indeed.lsmtree.core.ItUtil;
import com.indeed.lsmtree.core.Store;
import fj.F;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
* @author jplaisance
*/
public final class StoreCat {

    private static final Logger log = Logger.getLogger(StoreCat.class);

    public static <K,V> void cat(final Store<K, V> store, final K start, boolean startInclusive, final K end, final boolean endInclusive, Stringifier<K> keyStringifier, Stringifier<V> valueStringifier) throws IOException {
        Iterator<Store.Entry<K,V>> it = store.iterator(start, startInclusive);
        Iterator<Store.Entry<K,V>> iterator = end == null ? it : ItUtil.span(new F<Store.Entry<K, V>, Boolean>() {
            public Boolean f(final Store.Entry<K, V> kvEntry) {
                final int cmp = store.getComparator().compare(kvEntry.getKey(), end);
                return cmp < 0 || cmp == 0 && endInclusive;
            }
        }, it)._1();
        Map<String, Object> map = Maps.newLinkedHashMap();
        ObjectMapper mapper = new ObjectMapper();
        while (iterator.hasNext()) {
            Store.Entry<K, V> next = iterator.next();
            map.clear();
            map.put("key", keyStringifier.toString(next.getKey()));
            map.put("value", valueStringifier.toString(next.getValue()));
            System.out.println(mapper.writeValueAsString(map));
        }
    }

    public static <K,V> String md5(final Store<K, V> store, K start, boolean startInclusive, final K end, final boolean endInclusive) throws IOException {
        MD5OutputStream md5 = new MD5OutputStream(ByteStreams.nullOutputStream());
        DataOutputStream out = new DataOutputStream(md5);
        final Serializer<K> keySerializer = store.getKeySerializer();
        final Serializer<V> valueSerializer = store.getValueSerializer();
        Iterator<Store.Entry<K,V>> it = store.iterator(start, startInclusive);
        Iterator<Store.Entry<K,V>> iterator = end == null ? it : ItUtil.span(new F<Store.Entry<K, V>, Boolean>() {
            public Boolean f(final Store.Entry<K, V> kvEntry) {
                final int cmp = store.getComparator().compare(kvEntry.getKey(), end);
                return cmp < 0 || cmp == 0 && endInclusive;
            }
        }, it)._1();
        while (iterator.hasNext()) {
            Store.Entry<K, V> next = iterator.next();
            keySerializer.write(next.getKey(), out);
            valueSerializer.write(next.getValue(), out);
        }
        return md5.getHashString();
    }
}
