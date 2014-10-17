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
 package com.indeed.lsmtree.core;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.Ints;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * @author jplaisance
 */
public final class MergingIterator<K,V> extends AbstractIterator<Generation.Entry<K,V>> {
    private static final Logger log = Logger.getLogger(MergingIterator.class);

    private final PriorityQueue<PeekingIterator<EntryAndGenerationId<K,V>>> heap;

    private final List<PeekingIterator<EntryAndGenerationId<K,V>>> temp = Lists.newArrayList();

    private final Comparator<K> keyComparator;

    public MergingIterator(final Collection<Iterator<Generation.Entry<K,V>>> iterators, final Comparator<K> keyComparator) {
        this.keyComparator = keyComparator;
        Comparator<PeekingIterator<EntryAndGenerationId<K,V>>> comparator = new Comparator<PeekingIterator<EntryAndGenerationId<K,V>>>() {
            @Override
            public int compare(PeekingIterator<EntryAndGenerationId<K,V>> o1, PeekingIterator<EntryAndGenerationId<K,V>> o2) {
                EntryAndGenerationId<K,V> a = o1.peek();
                EntryAndGenerationId<K,V> b = o2.peek();
                int cmp = keyComparator.compare(a.entry.getKey(), b.entry.getKey());
                if (cmp != 0) return cmp;
                return Ints.compare(a.generationId, b.generationId);
            }
        };
        heap = new PriorityQueue<PeekingIterator<EntryAndGenerationId<K,V>>>(iterators.size(), comparator);
        int i = 0;
        for (final Iterator<Generation.Entry<K,V>> iterator : iterators) {
            final int generationId = i;
            PeekingIterator<EntryAndGenerationId<K, V>> iter = Iterators.peekingIterator(new Iterator<EntryAndGenerationId<K, V>>() {

                Iterator<Generation.Entry<K, V>> it = iterator;

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public EntryAndGenerationId<K, V> next() {
                    return new EntryAndGenerationId<K, V>(it.next(), generationId);
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            });
            if (iter.hasNext()) {
                heap.add(iter);
            }
            i++;
        }
    }

    @Override
    protected Generation.Entry<K, V> computeNext() {
        if (heap.isEmpty()) {
            return endOfData();
        }

        PeekingIterator<EntryAndGenerationId<K,V>> first = heap.poll();
        EntryAndGenerationId<K,V> ret = first.next();
        if (first.hasNext()) {
            temp.add(first);
        }
        while (!heap.isEmpty() && keyComparator.compare(ret.entry.getKey(), heap.peek().peek().entry.getKey()) == 0) {
            PeekingIterator<EntryAndGenerationId<K, V>> iter = heap.poll();
            iter.next();
            if (iter.hasNext()) {
                temp.add(iter);
            }
        }
        heap.addAll(temp);
        temp.clear();
        return ret.entry;
    }

    private static final class EntryAndGenerationId<K,V> {
        final Generation.Entry<K, V> entry;
        final int generationId;

        private EntryAndGenerationId(Generation.Entry<K, V> entry, int generationId) {
            this.entry = entry;
            this.generationId = generationId;
        }
    }
}
