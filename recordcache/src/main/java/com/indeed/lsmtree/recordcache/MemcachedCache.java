package com.indeed.lsmtree.recordcache;

import com.google.common.collect.Maps;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Longs;
import com.indeed.util.serialization.Serializer;
import com.indeed.util.serialization.Stringifier;
import net.spy.memcached.CachedData;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.transcoders.Transcoder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author jplaisance
 */
public final class MemcachedCache<K,V> implements Closeable {

    private static final Logger log = Logger.getLogger(MemcachedCache.class);

    private static final int CACHE_EXPIRY_SECONDS = (int)TimeUnit.HOURS.toSeconds(6);

    private final MemcachedClient memcache;

    private final InetSocketAddress host;

    private final String prefix;

    private final Stringifier<K> keyStringifier;
    
    private final Transcoder<V> valueTranscoder;

    private final ArrayBlockingQueue<Future<Boolean>> addFutureQueue = new ArrayBlockingQueue<Future<Boolean>>(10000);

    private final ArrayBlockingQueue<Future<Boolean>> setFutureQueue = new ArrayBlockingQueue<Future<Boolean>>(10000);

    private final AtomicBoolean run = new AtomicBoolean(true);

    private static final Transcoder<byte[]> identityTranscoder = new Transcoder<byte[]>() {
        @Override
        public boolean asyncDecode(final CachedData cachedData) {
            return false;
        }

        @Override
        public CachedData encode(final byte[] bytes) {
            return new CachedData(0, bytes, bytes.length);
        }

        @Override
        public byte[] decode(final CachedData cachedData) {
            return cachedData.getData();
        }

        @Override
        public int getMaxSize() {
            return Integer.MAX_VALUE;
        }
    };

    public static<K,V> MemcachedCache<K,V> create(String host, int port, String prefix, Stringifier<K> keyStringifier, Serializer<V> valueSerializer) throws IOException {
        InetSocketAddress address = new InetSocketAddress(host, port);
        MemcachedClient memcache = new MemcachedClient(address);
        return new MemcachedCache<K,V>(memcache, address, prefix, keyStringifier, valueSerializer);
    }

    MemcachedCache(
            final MemcachedClient memcache,
            final InetSocketAddress address,
            final String prefix,
            final Stringifier<K> keyStringifier,
            final Serializer<V> valueSerializer
    ) throws IOException {
        this.memcache = memcache;
        this.prefix = prefix;
        this.keyStringifier = keyStringifier;
        this.host = address;
        valueTranscoder = new Transcoder<V>() {
            @Override
            public boolean asyncDecode(CachedData cachedData) {
                return false;
            }

            @Override
            public CachedData encode(V v) {
                ByteArrayDataOutput out = ByteStreams.newDataOutput();
                try {
                    valueSerializer.write(v, out);
                    return identityTranscoder.encode(out.toByteArray());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public V decode(CachedData cachedData) {
                byte[] bytes = identityTranscoder.decode(cachedData);
                ByteArrayDataInput in = ByteStreams.newDataInput(bytes);
                try {
                    return valueSerializer.read(in);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public int getMaxSize() {
                return Integer.MAX_VALUE;
            }
        };
        final Thread addFutureChecker = new Thread(
                new FutureQueueChecker(run, addFutureQueue, "memcached add failed, key already exists in cache", Level.INFO),
                "addFutureChecker"
        );
        addFutureChecker.setDaemon(true);
        addFutureChecker.start();
        final Thread setFutureChecker = new Thread(
                new FutureQueueChecker(run, setFutureQueue, "memcached set failed, this should never happen", Level.ERROR),
                "setFutureChecker"
        );
        setFutureChecker.setDaemon(true);
        setFutureChecker.start();
    }

    public void putInCache(K key, V value, boolean addOnly) {
        putInCache(key, value, addOnly, -1);
    }

    public void putInCache(K key, V value, boolean addOnly, int expirationTime) {
        final String memcacheKey = prefix + keyStringifier.toString(key);
        try {
            if (addOnly) {
                final OperationFuture<Boolean> add = memcache.add(memcacheKey, expirationTime < 0 ? CACHE_EXPIRY_SECONDS : expirationTime, value, valueTranscoder);
                addFutureQueue.put(add);
            } else {
                final OperationFuture<Boolean> set = memcache.set(memcacheKey, expirationTime < 0 ? Integer.MAX_VALUE : expirationTime, value, valueTranscoder);
                setFutureQueue.put(set);
            }
        } catch (InterruptedException e) {
            log.error("interrupted while queueing", e);
        }
    }

    public Map<K,V> getFromCache(Collection<K> keys, CacheStats cacheStats) {
        if (keys.isEmpty()) {
            log.debug("got empty request");
            return Collections.emptyMap();
        }
        Map<K,V> results = Maps.newHashMapWithExpectedSize(keys.size());

        String[] memcachedKeys = new String[keys.size()];
        int i = 0;
        for (K key : keys) {
            memcachedKeys[i++] = prefix+ keyStringifier.toString(key);
        }
        long start = System.nanoTime();
        Map<String, V> map;
        try {
            map = memcache.getBulk(Arrays.asList(memcachedKeys), valueTranscoder);
        } catch (Exception e) {
            log.error("error getting bulk values", e);
            map = Collections.emptyMap();
        }
        if (map == null) map = Collections.emptyMap();
        cacheStats.memcacheTime+=System.nanoTime()-start;

        for (Map.Entry<String, V> entry : map.entrySet()) {
            final V value = entry.getValue();
            if (value != null) {
                String memcachedKey = entry.getKey();
                final K key = keyStringifier.fromString(memcachedKey.substring(prefix.length(), memcachedKey.length()));
                results.put(key, value);
            }
        }
        cacheStats.memcacheHits+=results.size();
        if (log.isTraceEnabled()) {
            log.trace("Requested " + keys.size() + " items from cache, got " + results.size() + " items back");
        }
        return results;
    }

    public void delete(K key) {
        String str = keyStringifier.toString(key);
        if (log.isTraceEnabled()) {
            log.trace("Deleting key " + str);
        }
        memcache.delete(prefix + str);
    }

    public Map<String, String> getStats() {
        final Map<String, String> retVal = memcache.getStats().get(host);
        if (retVal != null) {
            return retVal;
        } else {
            return Collections.emptyMap();
        }
    }

    public void shutdown() {
        if (log.isDebugEnabled()) {
            log.debug("Memcached Stats: " + memcache.getStats());
        }
        memcache.shutdown();
    }

    @Override
    public String toString() {
        return "[MemcachedCache: " + memcache + "]";
    }

    @Override
    public void close() throws IOException {
        run.set(false);
        shutdown();
    }

    public boolean checkAvailability(String key) {
        long time = System.nanoTime();
        key += "-"+UUID.randomUUID().toString();
        OperationFuture<Boolean> future = memcache.set(key, CACHE_EXPIRY_SECONDS, Longs.toByteArray(time), identityTranscoder);
        try {
            if (!future.get()) return false;
        } catch (Exception e) {
            return false;
        }
        byte[] bytes = memcache.get(key, identityTranscoder);
        memcache.delete(key);
        return bytes != null && Longs.fromByteArray(bytes) == time;
    }

    private static class FutureQueueChecker implements Runnable {

        private final AtomicBoolean run;

        private final BlockingQueue<Future<Boolean>> queue;

        private final String failedMessage;

        private final Priority failureLevel;

        private FutureQueueChecker(AtomicBoolean run, BlockingQueue<Future<Boolean>> queue, String failedMessage, Level failureLevel) {
            this.run = run;
            this.queue = queue;
            this.failedMessage = failedMessage;
            this.failureLevel = failureLevel;
        }

        public void run() {
            while (run.get()) {
                try {
                    final Future<Boolean> put = queue.poll(1, TimeUnit.SECONDS);
                    if (put != null) {
                        try {
                            if (!put.get()) {
                                log.log(failureLevel, failedMessage);
                            }
                        } catch (ExecutionException e) {
                            log.error("exception executing memcached operation in thread: "+Thread.currentThread().getName(), e);
                        }
                    }
                } catch (Exception e) {
                    log.error("exception executing memcached operation in thread: "+Thread.currentThread().getName(), e);
                }
            }
        }
    }
}
