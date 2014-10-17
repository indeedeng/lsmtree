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

import com.google.common.base.Throwables;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.util.core.hash.MurmurHash;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.serialization.Serializer;
import com.indeed.util.mmap.Memory;
import com.indeed.util.mmap.NativeBuffer;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jplaisance
 */
public final class BloomFilter {

    private static final Logger log = Logger.getLogger(BloomFilter.class);

    private static final int NUM_HASHES = 6;

    //must be a power of 2 for offset and mask calculations to be correct
    private static final long PAGE_SIZE = 65536;

    private static final long PAGE_OFFSET_MASK = PAGE_SIZE-1;

    private static final long PAGE_BITS = Long.bitCount(PAGE_OFFSET_MASK);

    private static final long ADDRESS_MASK = ~PAGE_OFFSET_MASK;

    public static final class NotEnoughMemoryException extends Exception {

        public NotEnoughMemoryException(final String message) {
            super(message);
        }
    }

    public static class Writer {

        public static <K> void write(MemoryManager memoryManager, File file, Iterator<? extends Generation.Entry<K, ?>> iterator, Serializer<K> keySerializer, long size)
                throws IOException, NotEnoughMemoryException {
            final MemoryManager.AddressSpace addressSpace = memoryManager.open(file, size);
            final Memory[] pages = new Memory[(int)((size-1) >> PAGE_BITS)+1];
            long pageIndex = 0;
            try {
                for (pageIndex = 0; pageIndex < size; pageIndex += PAGE_SIZE) {
                    final Memory page = addressSpace.getPageForWriting(pageIndex);
                    if (page == null) {
                        throw new NotEnoughMemoryException("insufficient memory available to write bloom filter, allocated "+pageIndex+" bytes before failing");
                    }
                    pages[((int)(pageIndex >>> PAGE_BITS))] = page;
                }
            } catch (Throwable t) {
                for (long j = 0; j < pageIndex; j += PAGE_SIZE) {
                    addressSpace.releasePage(j);
                }
                addressSpace.close();
                Throwables.propagateIfInstanceOf(t, IOException.class);
                Throwables.propagateIfInstanceOf(t, NotEnoughMemoryException.class);
                throw Throwables.propagate(t);
            }
            try {
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                final DataOutputStream dataOut = new DataOutputStream(out);
                while (iterator.hasNext()) {
                    final Generation.Entry<K, ?> next = iterator.next();
                    final K key = next.getKey();
                    out.reset();
                    keySerializer.write(key, dataOut);
                    final byte[] bytes = out.toByteArray();
                    int previousHash = MurmurHash.SEED64;
                    for (int i = 0; i < NUM_HASHES; i++) {
                        long hash = getHash(bytes, previousHash);
                        previousHash = (int)hash;
                        hash = hash % (size*8);
                        final long byteIndex = hash>>>3;
                        final int bitIndex = (int)(hash&7);
                        final Memory memory = pages[((int)(byteIndex >>> PAGE_BITS))];
                        int current = memory.getByte(byteIndex & PAGE_OFFSET_MASK)&0xFF;
                        current |= 1<<bitIndex;
                        memory.putByte(byteIndex & PAGE_OFFSET_MASK, (byte)current);
                    }
                }
            } finally {
                for (long i = 0; i < size; i += PAGE_SIZE) {
                    addressSpace.releasePage(i);
                }
                addressSpace.close();
            }
        }
    }

    public static class Reader<K> implements Closeable {

        private final MemoryManager.AddressSpace addressSpace;

        private final long size;

        private final Serializer<K> keySerializer;

        public Reader(MemoryManager memoryManager, File file, Serializer<K> keySerializer) throws IOException {
            this.keySerializer = keySerializer;
            size = file.length();
            this.addressSpace = memoryManager.openReadOnly(file, size);
        }

        public boolean contains(K key) {
            try {
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                final DataOutputStream dataOut = new DataOutputStream(out);
                keySerializer.write(key, dataOut);
                final byte[] bytes = out.toByteArray();
                int previousHash = MurmurHash.SEED64;
                int loadedFilters = 0;
                for (int i = 0; i < NUM_HASHES; i++) {
                    long hash = getHash(bytes, previousHash);
                    previousHash = (int)hash;
                    hash = hash % (size*8);
                    final long byteIndex = hash>>>3;
                    final int bitIndex = (int)(hash&7);
                    final Memory page = addressSpace.getPage(byteIndex);
                    if (page == null) continue;
                    loadedFilters++;
                    final int current = page.getByte(byteIndex & PAGE_OFFSET_MASK)&0xFF;
                    addressSpace.releasePage(byteIndex);
                    if ((current & (1<<bitIndex)) == 0) {
                        addressSpace.incUsefulCount();
                        addressSpace.incTotalCount();
                        return false;
                    }
                }
                if (loadedFilters == NUM_HASHES) addressSpace.incTotalCount();
                return true;
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void close() throws IOException {
            addressSpace.close();
        }

        public long sizeInBytes() {
            return size;
        }
    }

    private static long getHash(byte[] bytes, int seedValue) {
        long hash = MurmurHash.hash64(bytes, seedValue);
        if (hash < 0) hash = ~hash;
        return hash;
    }

    public static final class MemoryManager implements Closeable {

        private static final AtomicInteger THREAD_NUMBERER = new AtomicInteger(0);

        private final Set<AddressSpace> addressSpaces = Sets.newLinkedHashSet();
        private final NativeBuffer physicalMemory;
        private final PageTableEntry[] activePages;
        private int activePagePointer;
        private int activePagesEnd;
        private final List<Memory> freePages = Lists.newArrayList();
        private final AtomicBoolean runCleaner = new AtomicBoolean(true);
        private final boolean mLock;

        public MemoryManager(long physicalSize, boolean mLock) {
            this.mLock = mLock;
            physicalSize = physicalSize & ADDRESS_MASK;
            physicalMemory = new NativeBuffer(physicalSize, ByteOrder.LITTLE_ENDIAN);
            if (mLock) physicalMemory.mlock(0, physicalSize);
            activePages = new PageTableEntry[(int)(physicalSize >>> PAGE_BITS)];
            for (long i = 0; i < physicalSize; i += PAGE_SIZE) {
                freePages.add(physicalMemory.memory().slice(i, PAGE_SIZE));
            }
            final Thread cleaner = new Thread(new Runnable() {

                @Override
                public void run() {

                    synchronized (activePages) {
                        while (runCleaner.get()) {
                            final List<ScoredPageTableEntry> scoredActivePages = Lists.newArrayListWithCapacity(activePages.length);
                            final PriorityQueue<ScoredPageTableEntry> bestInactivePages =
                                    new PriorityQueue<ScoredPageTableEntry>(
                                            10,
                                            SCORE_COMPARATOR
                                    );

                            int activeIndex = 0;
                            //goal of stuff in this block is to replace the worst active pages with the best inactive pages
                            synchronized (addressSpaces) {

                                //first create list of active pages and heap of best activePages.length inactive pages
                                for (AddressSpace addressSpace : addressSpaces) {
                                    final double usefulness = addressSpace.useful.doubleValue()/addressSpace.total.get();
                                    for (PageTableEntry pte : addressSpace.pageTable) {
                                        synchronized (pte) {
                                            final double score = pte.requestsCounter * usefulness;
                                            if (pte.memory != null) {
                                                scoredActivePages.add(new ScoredPageTableEntry(score, pte));
                                            } else {
                                                if (bestInactivePages.size() < activePages.length) {
                                                    bestInactivePages.add(new ScoredPageTableEntry(score, pte));
                                                } else if (score < bestInactivePages.peek().score) {
                                                    bestInactivePages.poll();
                                                    bestInactivePages.add(new ScoredPageTableEntry(score, pte));
                                                }
                                            }
                                        }
                                    }
                                }

                                //convert heap of best active pages into a list so that we can access the best ones (instead of the worst ones)
                                final List<ScoredPageTableEntry> scoredInactivePages = Lists.newArrayListWithCapacity(bestInactivePages.size());
                                while (!bestInactivePages.isEmpty()) {
                                    scoredInactivePages.add(bestInactivePages.poll());
                                }

                                //if there are free pages, load the best inactive pages into the free pages and move them to the active set
                                synchronized (freePages) {
                                    while (!scoredInactivePages.isEmpty() && !freePages.isEmpty()) {
                                        final ScoredPageTableEntry scoreAndPte = scoredInactivePages.remove(scoredInactivePages.size()-1);
                                        final PageTableEntry pte = scoreAndPte.pageTableEntry;
                                        synchronized (pte) {
                                            if (pte.memory == null) {
                                                final Memory freePage = freePages.remove(freePages.size() - 1);
                                                try {
                                                    pte.addressSpace.loadPage(pte.index, freePage);
                                                    pte.memory = freePage;
                                                } catch (IOException e) {
                                                    log.error("error", e);
                                                }
                                            }
                                        }
                                        scoredActivePages.add(scoreAndPte);
                                    }
                                }

                                //sort the active pages by score
                                Collections.sort(scoredActivePages, SCORE_COMPARATOR);

                                //replace bad active pages with good inactive pages
                                int inactiveIndex = scoredInactivePages.size()-1;
                                Memory freePage = null;
                                while (inactiveIndex >= 0) {
                                    final ScoredPageTableEntry inactiveEntry = scoredInactivePages.get(inactiveIndex);
                                    //get a free page
                                    if (freePage == null) {
                                        if (activeIndex >= scoredActivePages.size()) {
                                            //out of freeable pages
                                            break;
                                        }
                                        final ScoredPageTableEntry activeEntry = scoredActivePages.get(activeIndex);
                                        activeIndex++;
                                        if (inactiveEntry.score < activeEntry.score) {
                                            //all remaining inactive entries are worse than all remaining active entries
                                            break;
                                        }
                                        final PageTableEntry activePte = activeEntry.pageTableEntry;
                                        //try to free the page
                                        synchronized (activePte) {
                                            try {
                                                freePage = activePte.addressSpace.freePage(activePte.index);
                                                if (freePage == null) {
                                                    //couldn't free the page
                                                    continue;
                                                }
                                            } catch (IOException e) {
                                                log.error("error", e);
                                                continue;
                                            }
                                        }
                                    }
                                    inactiveIndex--;
                                    final PageTableEntry inactivePte = inactiveEntry.pageTableEntry;
                                    //load data into page
                                    synchronized (inactivePte) {
                                        if (inactivePte.memory != null) {
                                            //oops. this page is already present
                                            continue;
                                        }
                                        try {
                                            inactivePte.addressSpace.loadPage(inactivePte.index, freePage);
                                        } catch (IOException e) {
                                            //that didn't work out so well
                                            log.error("error", e);
                                            continue;
                                        }
                                        inactivePte.memory = freePage;
                                        freePage = null;
                                    }
                                    scoredActivePages.set(activeIndex-1, inactiveEntry);
                                }
                                //downsample usage statistics so that current values take precedence
                                for (AddressSpace addressSpace : addressSpaces) {
                                    long useful;
                                    while (true) {
                                        useful = addressSpace.useful.get();
                                        final long update = useful * 9 / 10;
                                        if (addressSpace.useful.compareAndSet(useful, Math.max(update, 1))) break;
                                    }
                                    long total;
                                    while (true) {
                                        total = addressSpace.total.get();
                                        final long update = total * 9 / 10;
                                        if (addressSpace.total.compareAndSet(total, Math.max(update, 2))) break;
                                    }
                                    if (log.isDebugEnabled()) log.debug(
                                            "usefulness for filter " +
                                                    addressSpace.file.getPath() +
                                                    " - useful: " +
                                                    useful +
                                                    " total: " +
                                                    total +
                                                    " usefulness: " +
                                                    (double)useful / total
                                    );
                                    for (PageTableEntry pte : addressSpace.pageTable) {
                                        synchronized (pte) {
                                            pte.requestsCounter = pte.requestsCounter*9/10;
                                        }
                                    }
                                }
                            }

                            //sort active pages again (this can be optimized if it's an issue)
                            Collections.sort(scoredActivePages, SCORE_COMPARATOR);

                            activePagePointer = 0;
                            activePagesEnd = Math.min(scoredActivePages.size(), activePages.length);

                            //copy active pages into active pages list and set pointers
                            for (int i = 0; i < activePagesEnd; i++) {
                                activePages[i] = scoredActivePages.get(i).pageTableEntry;
                            }

                            try {
                                activePages.wait(10000);
                            } catch (InterruptedException e) {
                                //ignore
                            }
                        }
                    }
                }
            }, "PageTableCleanerThread-"+THREAD_NUMBERER.getAndIncrement());
            cleaner.setDaemon(true);
            cleaner.start();
        }

        public AddressSpace openReadOnly(File file, long length) throws IOException {
            final AddressSpace addressSpace = new AddressSpace(file, length, true);
            synchronized (addressSpaces) {
                addressSpaces.add(addressSpace);
            }
            return addressSpace;
        }

        public AddressSpace open(File file, long length) throws IOException {
            final AddressSpace addressSpace = new AddressSpace(file, length, false);
            synchronized (addressSpaces) {
                addressSpaces.add(addressSpace);
            }
            return addressSpace;
        }

        private @Nullable Memory getFreePage() throws IOException {
            synchronized (freePages) {
                if (!freePages.isEmpty()) {
                    return freePages.remove(freePages.size()-1);
                }
            }
            synchronized (activePages) {
                for (;activePagePointer < activePagesEnd; activePagePointer++) {
                    final PageTableEntry pte = activePages[activePagePointer];
                    final Memory freePage = pte.addressSpace.freePage(pte.index);
                    if (freePage != null) {
                        activePagePointer++;
                        //heuristic: if more than 20% of the active pages have been overwritten since the last page table clean, wake up the cleaner
                        if (activePagePointer > activePages.length/5) {
                            activePages.notify();
                        }
                        return freePage;
                    }
                }
                activePages.notify();
                return null;
            }
        }

        @Override
        public void close() {
            if (mLock) physicalMemory.munlock(0, physicalMemory.memory().length());
            runCleaner.set(false);
            Closeables2.closeQuietly(physicalMemory, log);
        }

        public final class AddressSpace implements Closeable {
            private final File file;
            private final RandomAccessFile raf;
            private final byte[] pageBuffer = new byte[(int)PAGE_SIZE];
            private final PageTableEntry[] pageTable;
            private final long length;
            private final AtomicLong useful = new AtomicLong(50);
            private final AtomicLong total = new AtomicLong(100);

            public AddressSpace(File file, long length, boolean readOnly) throws IOException {
                this.file = file;
                raf = new RandomAccessFile(file, readOnly ? "r" : "rw");
                this.length = length;
                if (length > raf.length()) {
                    raf.setLength(length);
                }
                final int numPages = (int)((length - 1) >> PAGE_BITS) + 1;
                pageTable = new PageTableEntry[numPages];
                for (int i = 0; i < numPages; i++) {
                    pageTable[i] = new PageTableEntry(this, i);
                }
            }

            private PageTableEntry getPageTableEntry(final long address) {
                return pageTable[((int)(address >>> PAGE_BITS))];
            }

            private void loadPage(int index, Memory freePage) throws IOException {
                final long address = (long)index << PAGE_BITS;
                if (log.isDebugEnabled()) log.debug("loading page in file " + file.getPath() + " at address " + address);
                final int pageLength = (int)Math.min(PAGE_SIZE, length - address);
                synchronized (raf) {
                    raf.seek(address);
                    raf.readFully(pageBuffer, 0, pageLength);
                    freePage.putBytes(0, pageBuffer, 0, pageLength);
                }
            }

            private void syncPage(int index) throws IOException {
                final PageTableEntry pte = pageTable[index];
                final long address = (long)index << PAGE_BITS;
                synchronized (pte) {
                    if (pte.dirty) {
                        if (log.isDebugEnabled()) log.debug("synchronizing page in file " + file.getPath() + " at address " + address);
                        final int pageLength = (int)Math.min(PAGE_SIZE, length-address);
                        synchronized (raf) {
                            pte.memory.getBytes(0, pageBuffer, 0, pageLength);
                            raf.seek(address);
                            raf.write(pageBuffer, 0, pageLength);
                        }
                        pte.dirty = false;
                    }
                }
            }

            private @Nullable Memory freePage(int index) throws IOException {
                final PageTableEntry pte = pageTable[index];
                synchronized (pte) {
                    if (pte.memory == null) {
                        return null;
                    }
                    if (pte.refCount > 0) {
                        return null;
                    }
                    if (log.isDebugEnabled()) log.debug("evicting page in file " + pte.addressSpace.file.getPath() + " at address " + ((long)pte.index << PAGE_BITS));
                    if (pte.dirty) {
                        syncPage(index);
                    }
                    final Memory ret = pte.memory;
                    pte.memory = null;
                    return ret;
                }
            }

            /**
             * gets memory for a page if it is currently paged in and increments its refcount
             * @param address address of page
             * @return memory for page or null if it is not currently paged in
             */
            public @Nullable Memory getPage(long address) {
                final PageTableEntry pageTableEntry = getPageTableEntry(address);
                synchronized (pageTableEntry) {
                    pageTableEntry.requestsCounter++;
                    final Memory memory = pageTableEntry.memory;
                    if (memory != null) {
                        pageTableEntry.refCount++;
                    }
                    return memory;
                }
            }

            /**
             * gets memory for a page, sets dirty bit, and increments its refcount
             * @param address address of page
             * @return memory for page or null if there is no memory available at this time
             * @throws IOException if page is paged out and an IOException occurs attempting to page it in
             */
            public @Nullable Memory getPageForWriting(long address) throws IOException {
                final PageTableEntry pte = getPageTableEntry(address);
                synchronized (pte) {
                    if (pte.memory != null) {
                        pte.dirty = true;
                        pte.refCount++;
                        return pte.memory;
                    }
                }
                final Memory freePage = getFreePage();
                if (freePage == null) return null;
                final Memory ret;
                synchronized (pte) {
                    if (pte.memory == null) {
                        loadPage((int)(address >>> PAGE_BITS), freePage);
                        pte.memory = freePage;
                        ret = freePage;
                    } else {
                        ret = pte.memory;
                    }
                    pte.dirty = true;
                    pte.refCount++;
                }
                if (ret != freePage) {
                    synchronized (freePages) {
                        freePages.add(freePage);
                    }
                }
                return ret;
            }

            /**
             * decrements refcount for a page
             * @param address address of page
             */
            public void releasePage(long address) {
                final PageTableEntry pte = getPageTableEntry(address);
                synchronized (pte) {
                    pte.refCount--;
                }
            }

            public void incUsefulCount() {
                useful.incrementAndGet();
            }

            public void incTotalCount() {
                total.incrementAndGet();
            }

            /**
             * writing to currently referenced pages during or after this call will result in undefined behaviour. you must call
             * getPageForWriting again after this call to set the dirty bit again.
             * @throws IOException in exceptional circumstances
             */
            public void sync() throws IOException {
                for (int i = 0; i < pageTable.length; i++) {
                    final PageTableEntry pte = pageTable[i];
                    synchronized (pte) {
                        if (pte.dirty) {
                            syncPage(i);
                        }
                    }
                }
                raf.getFD().sync();
            }

            /**
             * writing to or reading from referenced pages after this call will result in undefined behaviour
             * @throws IOException in exceptional circumstances
             */
            @Override
            public void close() throws IOException {
                try {
                    sync();
                } finally {
                    synchronized (addressSpaces) {
                        addressSpaces.remove(this);
                    }
                    for (PageTableEntry pte : pageTable) {
                        final Memory freePage;
                        synchronized (pte) {
                            if (pte.memory != null) {
                                final long address = (long)pte.index << PAGE_BITS;
                                if (log.isDebugEnabled()) log.debug("evicting page in file "+file.getPath()+" at address "+ address);
                                if (pte.dirty) {
                                    log.error("page in file "+file.getPath()+" at address "+address+" is dirty");
                                }
                                if (pte.refCount > 0) {
                                    log.error("page in file "+file.getPath()+" at address "+address+" is still in use, refcount: "+pte.refCount);
                                }
                            }
                            freePage = pte.memory;
                            pte.memory = null;
                        }
                        if (freePage != null) {
                            synchronized (freePages) {
                                freePages.add(freePage);
                            }
                        }
                    }
                    Closeables2.closeQuietly(raf, log);
                }
            }
        }

        private static final class PageTableEntry {
            long requestsCounter;
            int refCount;
            boolean dirty;
            Memory memory;
            final AddressSpace addressSpace;
            final int index;

            private PageTableEntry(final AddressSpace addressSpace, final int index) {
                this.addressSpace = addressSpace;
                this.index = index;
            }
        }

        private static final class ScoredPageTableEntry {
            final double score;
            final PageTableEntry pageTableEntry;

            private ScoredPageTableEntry(double score, PageTableEntry pageTableEntry) {
                this.score = score;
                this.pageTableEntry = pageTableEntry;
            }
        }

        private static final Comparator<ScoredPageTableEntry> SCORE_COMPARATOR = new Comparator<ScoredPageTableEntry>() {
            @Override
            public int compare(ScoredPageTableEntry o1, ScoredPageTableEntry o2) {
                return ComparisonChain.start()
                        .compare(o1.score, o2.score)
                        .result();
            }
        };
    }
}
