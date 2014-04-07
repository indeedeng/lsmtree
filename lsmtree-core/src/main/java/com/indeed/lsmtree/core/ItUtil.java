package com.indeed.lsmtree.core;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import fj.F;
import fj.F2;
import fj.P2;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * @author jplaisance
 */

public final class ItUtil {

    public static <A> Iterable<A> iterable(final Iterator<A> it) {
        return new Iterable<A>() {
            boolean used = false;
            @Override
            public Iterator<A> iterator() {
                if (used) throw new IllegalStateException("iterator may not be called more than once");
                used = true;
                return it;
            }
        };
    }

    public static <A> Iterable<A> iterable(final Iterator<A> it, boolean reusable) {
        if (reusable) {
            ArrayList<A> ret = new ArrayList<A>();
            for (A t : iterable(it)) {
                ret.add(t);
            }
            return ret;
        }
        return iterable(it);
    }

    public static <A,B> Iterator<B> map(final F<A,B> f, final Iterator<A> it) {
        return new AbstractIterator<B>() {
            @Override
            protected B computeNext() {
                if (it.hasNext()) return f.f(it.next());
                endOfData();
                return null;
            }
        };
    }

    public static <A,B> Iterable<B> map(final F<A,B> f, final Iterable<A> it) {
        return new Iterable<B>() {
            @Override
            public Iterator<B> iterator() {
                return map(f, it.iterator());
            }
        };
    }

    public static <A> Iterator<A> filter(final F<A,Boolean> f, final Iterator<A> it) {
        return new AbstractIterator<A>() {
            @Override
            protected A computeNext() {
                while (true) {
                    if (it.hasNext()){
                        A a = it.next();
                        if (f.f(a)) {
                            return a;
                        }
                    } else {
                        endOfData();
                        return null;
                    }
                }
            }
        };
    }

    public static <A> Iterable<A> filter(final F<A,Boolean> f, final Iterable<A> it) {
        return new Iterable<A>() {
            @Override
            public Iterator<A> iterator() {
                return filter(f, it.iterator());
            }
        };
    }

    public static <A,B> B fold(final F2<B,A,B> f, final B initial, final Iterable<A> it) {
        B b = initial;
        for (A a : it) {
            b = f.f(b,a);
        }
        return b;
    }

    public static <A,B> B fold(final F2<B,A,B> f, final B initial, final Iterator<A> it) {
        return fold(f, initial, iterable(it));
    }

    public static <A> P2<Iterator<A>, Iterator<A>> span(final F<A,Boolean> f, final Iterator<A> it) {
        return new P2<Iterator<A>, Iterator<A>>() {
            PeekingIterator<A> peekingIterator = Iterators.peekingIterator(it);
            boolean firstDone = false;
            Iterator<A> first = new AbstractIterator<A>() {
                @Override
                protected A computeNext() {
                    if (firstDone) throw new IllegalStateException("cannot access first iterator after second has been accessed");
                    if (peekingIterator.hasNext()) {
                        if (f.f(peekingIterator.peek())) {
                            return peekingIterator.next();
                        }
                    }
                    endOfData();
                    firstDone = true;
                    return null;
                }
            };
            Iterator<A> second = new AbstractIterator<A>() {
                @Override
                protected A computeNext() {
                    if (!firstDone) {
                        while (first.hasNext()) first.next();
                        firstDone = true;
                    }
                    if (peekingIterator.hasNext()) return peekingIterator.next();
                    endOfData();
                    return null;
                }
            };
            @Override
            public Iterator<A> _1() {
                return first;
            }

            @Override
            public Iterator<A> _2() {
                return second;
            }
        };
    }

    public static <A> P2<Iterator<A>, Iterator<A>> span(final F<A,Boolean> f, final Iterable<A> it) {
        return span(f, it.iterator());
    }

    public static <A> Iterator<Iterator<A>> groupBy(final F2<A,A,Boolean> f, final Iterator<A> iterator) {
        return new AbstractIterator<Iterator<A>>() {
            PeekingIterator<A> it = Iterators.peekingIterator(iterator);
            InvalidatableIterator<A> prev = null;
            @Override
            protected Iterator<A> computeNext() {
                if (it.hasNext()) {
                    if (prev != null) {
                        while (prev.hasNext()) prev.next();
                        prev.invalidate();
                    }
                    prev = new InvalidatableIterator<A>() {
                        A a;
                        boolean initialized = false;
                        @Override
                        protected A computeNext1() {
                            if (!initialized) {
                                a = it.next();
                                initialized = true;
                                return a;
                            }
                            if (it.hasNext()) {
                                if (f.f(a, it.peek())) {
                                    a = it.next();
                                    return a;
                                }
                            }
                            endOfData();
                            return null;
                        }
                    };
                    return prev;
                }
                endOfData();
                return null;
            }
        };
    }

    public static <A> Iterator<Iterator<A>> groupBy(final F2<A,A,Boolean> f, final Iterable<A> it) {
        return groupBy(f, it.iterator());
    }

    public static <A> Iterator<A> intersperse(final A a, final Iterator<A> it) {
        return new AbstractIterator<A>() {
            boolean b = false;
            @Override
            protected A computeNext() {
                if (it.hasNext()) {
                    if (b) {
                        b = false;
                        return a;
                    }
                    b = true;
                    return it.next();
                }
                endOfData();
                return null;
            }
        };
    }

    public static <A> Iterable<A> intersperse(final A a, final Iterable<A> it) {
        return new Iterable<A>() {
            @Override
            public Iterator<A> iterator() {
                return intersperse(a, it.iterator());
            }
        };
    }

    public static <A> Iterator<A> intercalate(final Iterator<A> a, final Iterator<Iterator<A>> it) {
        return Iterables.concat(
                intersperse(
                        iterable(a, true), iterable(map(
                        new F<Iterator<A>, Iterable<A>>() {
                            @Override
                            public Iterable<A> f(final Iterator<A> aIterator) {
                                return iterable(aIterator);
                            }
                        }, it
                )))).iterator();
    }

    public static <A> Iterator<Iterator<A>> partition(final Iterator<A> a, final int count) {
        return new AbstractIterator<Iterator<A>>() {
            @Override
            protected Iterator<A> computeNext() {
                return new AbstractIterator<A>() {
                    int c = 0;
                    @Override
                    protected A computeNext() {
                        if (c == count || !a.hasNext()) {
                            endOfData();
                            return null;
                        }
                        return a.next();
                    }
                };
            }
        };
    }

    public static <A> Iterable<Iterable<A>> partition(final Iterable<A> a, final int count) {
        return new Iterable<Iterable<A>>() {
            @Override
            public Iterator<Iterable<A>> iterator() {
                final Iterator<A> it = a.iterator();
                return new AbstractIterator<Iterable<A>>() {
                    @Override
                    protected Iterable<A> computeNext() {
                        if (it.hasNext()) {
                            return new Iterable<A>() {
                                @Override
                                public Iterator<A> iterator() {
                                    return new AbstractIterator<A>() {
                                        int c = 0;
                                        @Override
                                        protected A computeNext() {
                                            if (c == count || !it.hasNext()) {
                                                endOfData();
                                                return null;
                                            }
                                            return it.next();
                                        }
                                    };
                                }
                            };
                        }
                        endOfData();
                        return null;
                    }
                };
            }
        };
    }

    private static abstract class InvalidatableIterator<E> extends AbstractIterator<E> {

        private boolean invalid = false;

        public final void invalidate() {
            invalid = true;
        }

        @Override
        protected final E computeNext() {
            if (invalid) throw new IllegalStateException("iterator has been invalidated");
            return computeNext1();
        }

        abstract protected E computeNext1();
    }

    public static <E> Iterator<E> merge(Collection<Iterator<E>> iterators, final Comparator<E> comparator) {
        Comparator<PeekingIterator<E>> heapComparator = new Comparator<PeekingIterator<E>>() {
            @Override
            public int compare(final PeekingIterator<E> o1, final PeekingIterator<E> o2) {
                return comparator.compare(o1.peek(), o2.peek());
            }
        };
        final ObjectHeapPriorityQueue<PeekingIterator<E>> heap = new ObjectHeapPriorityQueue<PeekingIterator<E>>(heapComparator);
        for (Iterator<E> iterator : iterators) {
            if (iterator.hasNext()) {
                if (iterator instanceof PeekingIterator) {
                    heap.enqueue((PeekingIterator<E>) iterator);
                } else {
                    heap.enqueue(Iterators.peekingIterator(iterator));
                }
            }
        }
        return new AbstractIterator<E>() {
            @Override
            protected E computeNext() {
                if (heap.isEmpty()) {
                    endOfData();
                    return null;
                }
                PeekingIterator<E> iterator = heap.first();
                E ret = iterator.next();
                if (iterator.hasNext()) {
                    heap.changed();
                } else {
                    heap.dequeue();
                }
                return ret;
            }
        };
    }

    public static void main(String[] args) {
        Integer[] ints = new Integer[]{1,1,1,2,2,3,3,3,3,4,4,5,5,6,7,8,9,10,10,11};
        List<Integer> intList = Arrays.asList(ints);
        final Iterator<Iterator<Integer>> it = groupBy(
                new F2<Integer, Integer, Boolean>() {

                    @Override
                    public Boolean f(final Integer integer, final Integer integer1) {
                        return integer.equals(integer1);
                    }
                }, intList
        );
        F<Iterator<Integer>, String> concat = new F<Iterator<Integer>, String>() {
            @Override
            public String f(final Iterator<Integer> integerIterator) {
                return fold(new F2<String,String,String>(){
                    @Override
                    public String f(final String s, final String s1) {
                        return s+s1;
                    }
                }, "",(intersperse(", ", map(new F<Integer, String>() {

                    @Override
                    public String f(final Integer integer) {
                        return String.valueOf(integer);
                    }
                }, integerIterator
                ))));
            }
        };
        for (Iterator<Integer> it1 : iterable(it)) {
            System.out.println(concat.f(it1));
        }
        P2<Iterator<Integer>, Iterator<Integer>> p = span(
                new F<Integer, Boolean>() {
                    @Override
                    public Boolean f(final Integer integer) {
                        return integer < 5;
                    }
                }, intList
        );
        System.out.println(concat.f(p._2()));
        Iterable<Integer> it2 = filter(
                new F<Integer, Boolean>() {
                    @Override
                    public Boolean f(final Integer integer) {
                        return integer % 2 == 0;
                    }
                }, intList
        );
        System.out.println(concat.f(it2.iterator()));
    }
}
