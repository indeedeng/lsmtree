package com.indeed.lsmtree.recordcache;

import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.util.core.Either;
import com.indeed.util.core.LongRecentEventsCounter;
import com.indeed.util.serialization.Serializer;
import com.indeed.util.serialization.Stringifier;
import fj.F;
import fj.P2;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jplaisance
 */
public final class MemcachedMappingRecordCache<A, B, C, D> implements RecordCache<C, D> {

    private static final Logger log = Logger.getLogger(MemcachedMappingRecordCache.class);

    private final PersistentRecordCache<A, B> baseCache;

    private final F<A, C> aToCFunction;

    private final F<C, A> cToAFunction;

    private final F<B, D> valueMapFunction;

    private final MemcachedCache<C, D> memcache;

    private final RecordLogDirectoryPoller.Functions memcacheUpdateFunctions;

    private final LongRecentEventsCounter memcacheMissCounter = new LongRecentEventsCounter(LongRecentEventsCounter.MINUTE_TICKER, 60);
    private final LongRecentEventsCounter lsmTreeMissCounter = new LongRecentEventsCounter(LongRecentEventsCounter.MINUTE_TICKER, 60);

    public MemcachedMappingRecordCache(PersistentRecordCache<A, B> baseCache, final F<A, C> aToCFunction, F<C, A> cToAFunction, final F<B, D> valueMapFunction, MemcachedCache<C, D> memcachedCache) {
        this.baseCache = baseCache;
        this.aToCFunction = aToCFunction;
        this.cToAFunction = cToAFunction;
        this.valueMapFunction = valueMapFunction;
        this.memcache = memcachedCache;
        memcacheUpdateFunctions = new RecordLogDirectoryPoller.Functions() {

            AtomicLong memcachedPutTime = new AtomicLong(0);

            AtomicLong memcachedDeleteTime = new AtomicLong(0);

            AtomicInteger memcachedPuts = new AtomicInteger(0);

            AtomicInteger memcachedDeletes = new AtomicInteger(0);

            AtomicInteger count = new AtomicInteger(0);

            @Override
            public void process(final long position, Operation op) throws IOException {
                count.incrementAndGet();
                if (count.get() % 1000 == 0) {
                    final int puts = memcachedPuts.get();
                    if (puts > 0) log.debug("avg memcached put time: " + memcachedPutTime.get() / puts / 1000d + " us");
                    final int deletes = memcachedDeletes.get();
                    if (deletes > 0) log.debug("avg memcached delete time: " + memcachedDeleteTime.get() / deletes / 1000d + " us");
                }
                if (op.getClass() == Put.class) {
                    final Put<A, B> put = (Put)op;
                    final long start = System.nanoTime();
                    memcache.putInCache(aToCFunction.f(put.getKey()), valueMapFunction.f(put.getValue()), false);
                    memcachedPutTime.addAndGet(System.nanoTime()-start);
                    memcachedPuts.incrementAndGet();
                } else if (op.getClass() == Delete.class) {
                    final Delete<A> delete = (Delete)op;
                    for (A a : delete.getKeys()) {
                        final long start = System.nanoTime();
                        memcache.delete(aToCFunction.f(a));
                        memcachedDeleteTime.addAndGet(System.nanoTime()-start);
                        memcachedDeletes.incrementAndGet();
                    }
                } else if (op.getClass() == Checkpoint.class) {
                    //ignore
                } else {
                    log.warn("operation class unknown");
                }
            }

            @Override
            public void sync() throws IOException {}
        };
    }

    @Override
    public D get(final C key, final CacheStats cacheStats) {
        final Map<C, D> result = getAll(Collections.singleton(key), cacheStats);
        if (result.containsKey(key)) return result.get(key);
        return null;
    }

    @Override
    public Map<C, D> getAll(final Collection<C> keys, final CacheStats cacheStats) {
        final Map<C, D> results = memcache.getFromCache(keys, cacheStats);
        final int size = results.size();
        if (size < keys.size()) {
            final Set<A> missingKeys = Sets.newHashSet();
            for (C key : keys) {
                if (!results.containsKey(key)) {
                    missingKeys.add(cToAFunction.f(key));
                    synchronized (memcacheMissCounter) {
                       memcacheMissCounter.increment();
                    }
                }
            }
            if (missingKeys.size() > 0) log.info("memcached misses: "+missingKeys);
            final Map<A, B> baseCacheResults = baseCache.getAll(missingKeys, cacheStats);
            for (A a : missingKeys) {
                final B b = baseCacheResults.get(a);
                if (b == null) {
                    synchronized (lsmTreeMissCounter) {
                       lsmTreeMissCounter.increment();
                    }
                    log.warn("key "+aToCFunction.f(a)+" not found in underlying store");
                    continue;
                }
                final C c = aToCFunction.f(a);
                final D d = valueMapFunction.f(b);
                results.put(c, d);
                memcache.putInCache(c, d, true);
            }
        }
        cacheStats.persistentStoreHits = results.size()-size;
        log.debug("persistent store hits: " + (results.size() - size));
        cacheStats.misses = keys.size()-results.size();
        log.debug("misses: "+(keys.size()-results.size()));
        return results;
    }

    @Override
    public RecordLogDirectoryPoller.Functions getFunctions() {
        return memcacheUpdateFunctions;
    }

    public void removeFromCache(C key) {
        memcache.delete(key);
    }

    public Map<String, String> getStats() {
        return memcache.getStats();
    }

    public void prime(Iterator<C> keys, @Nullable final PrimerStatus status) throws IndexReadException {
        final Iterator<List<C>> partitions = Iterators.partition(keys, 1000);
        final Iterator<A> iterator = Iterators.concat(new AbstractIterator<Iterator<A>>() {
            @Override
            protected Iterator<A> computeNext() {
                try {
                    if (!partitions.hasNext()) {
                        endOfData();
                        return null;
                    }
                    final CacheStats cacheStats = new CacheStats();
                    final List<C> partition = partitions.next();
                    final Map<C, D> results = memcache.getFromCache(partition, cacheStats);
                    final List<A> list = Lists.newArrayListWithExpectedSize(partition.size() - results.size());
                    int foundInCache = 0;
                    for (C c : partition) {
                        if (!results.containsKey(c)) {
                            list.add(cToAFunction.f(c));
                        } else {
                            foundInCache++;
                        }
                    }
                    if (status != null) {
                        status.primerProgress.addAndGet(foundInCache);
                        status.primerPrimed.addAndGet(foundInCache);
                    }
                    return list.iterator();
                } catch (Throwable t) {
                    log.error("error", t);
                    throw Throwables.propagate(t);
                }
            }
        });
        final Iterator<Either<Exception,P2<A,B>>> streaming = baseCache.getStreaming(iterator, status != null ? status.primerProgress : null, status != null ? status.primerSkipped : null);
        log.info("store lookup iterator initialized");
        log.info("starting store lookups");
        while (streaming.hasNext()) {
            final Either<Exception, P2<A, B>> next = streaming.next();
            try {
                final P2<A, B> value = next.get();
                memcache.putInCache(aToCFunction.f(value._1()), valueMapFunction.f(value._2()), false);
                if (status != null) {
                    status.primerPrimed.incrementAndGet();
                    status.primerProgress.incrementAndGet();
                }
            } catch (IndexReadException e) {
                log.error("error", e);
                throw e;
            } catch (Exception e) {
                log.error("exception during priming", e);
                if (status != null) {
                    status.primerErrors.incrementAndGet();
                }
            }
        }
        log.info("docstore lookups complete");
    }

    @Override
    public void close() throws IOException {
        memcache.close();
        //leave it up to someone else to close underyingCache
    }

    public boolean checkAvailability(String key) {
        return memcache.checkAvailability(key);
    }

    public String getMemcacheMissCounter() {
        return memcacheMissCounter.toString();
    }

    public String getLsmTreeMissCounter() {
        return lsmTreeMissCounter.toString();
    }

    public static final class Builder<A,B,C,D> {
        private Stringifier<C> keyStringifier;
        private Serializer<C> keySerializer;
        private Serializer<D> valueSerializer;
        private String memcacheHost;
        private int memcachePort = -1;
        private String memcacheKeyPrefix = "";
        private PersistentRecordCache<A, B> baseCache;
        private F<A, C> aToCFunction;
        private F<C, A> cToAFunction;
        private F<B, D> valueMapFunction;

        public MemcachedMappingRecordCache<A,B,C,D> build() throws IOException {
            if (keyStringifier == null) throw new IllegalArgumentException("keyStringifier must be set");
            if (keySerializer == null) throw new IllegalArgumentException("keySerializer must be set");
            if (valueSerializer == null) throw new IllegalArgumentException("valueSerializer must be set");
            if (memcacheHost == null) throw new IllegalArgumentException("memcacheHost must be set");
            if (memcachePort < 0) throw new IllegalArgumentException("memcachePort must be set");
            final MemcachedCache<C, D> memcache = MemcachedCache.create(memcacheHost, memcachePort, memcacheKeyPrefix, keyStringifier, valueSerializer);
            return new MemcachedMappingRecordCache<A, B, C, D>(baseCache, aToCFunction, cToAFunction, valueMapFunction, memcache);
        }

        public Builder setKeyStringifier(final Stringifier<C> keyStringifier) {
            this.keyStringifier = keyStringifier;
            return this;
        }

        public Builder setKeySerializer(final Serializer<C> keySerializer) {
            this.keySerializer = keySerializer;
            return this;
        }

        public Builder setValueSerializer(final Serializer<D> valueSerializer) {
            this.valueSerializer = valueSerializer;
            return this;
        }

        public Builder setMemcacheHost(final String memcacheHost) {
            this.memcacheHost = memcacheHost;
            return this;
        }

        public Builder setMemcachePort(final int memcachePort) {
            this.memcachePort = memcachePort;
            return this;
        }

        public Builder setMemcacheKeyPrefix(final String memcacheKeyPrefix) {
            this.memcacheKeyPrefix = memcacheKeyPrefix;
            return this;
        }

        public Builder setBaseCache(final PersistentRecordCache<A, B> baseCache) {
            this.baseCache = baseCache;
            return this;
        }

        public Builder setaToCFunction(final F<A, C> aToCFunction) {
            this.aToCFunction = aToCFunction;
            return this;
        }

        public Builder setcToAFunction(final F<C, A> cToAFunction) {
            this.cToAFunction = cToAFunction;
            return this;
        }

        public Builder setValueMapFunction(final F<B, D> valueMapFunction) {
            this.valueMapFunction = valueMapFunction;
            return this;
        }
    }

    public static class PrimerStatus {
        private final AtomicLong primerErrors;
        private final AtomicInteger primerProgress;
        private final AtomicInteger primerSkipped;
        private final AtomicInteger primerPrimed;

        public PrimerStatus(AtomicLong primerErrors, AtomicInteger primerProgress, AtomicInteger primerSkipped, AtomicInteger primerPrimed) {
            this.primerErrors = primerErrors;
            this.primerProgress = primerProgress;
            this.primerSkipped = primerSkipped;
            this.primerPrimed = primerPrimed;
        }

        public AtomicLong getPrimerErrors() {
            return primerErrors;
        }

        public AtomicInteger getPrimerProgress() {
            return primerProgress;
        }

        public AtomicInteger getPrimerSkipped() {
            return primerSkipped;
        }

        public AtomicInteger getPrimerPrimed() {
            return primerPrimed;
        }

        @Override
        public String toString() {
            return "PrimerStatus{" +
                    "primerErrors=" +
                    primerErrors +
                    ", primerProgress=" +
                    primerProgress +
                    ", primerSkipped=" +
                    primerSkipped +
                    ", primerPrimed=" +
                    primerPrimed +
                    '}';
        }
    }
}
