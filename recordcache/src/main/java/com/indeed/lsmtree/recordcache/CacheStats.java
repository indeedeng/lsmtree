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

/**
* @author jplaisance
*/
public class CacheStats {
    int memcacheHits = 0;
    long memcacheTime = 0;
    int persistentStoreHits = 0;
    long indexTime = 0;
    long recordLogTime = 0;
    int misses = 0;
    int recordLogReadErrors = 0;
    int indexReadErrors = 0;

    public int getMemcacheHits() {
        return memcacheHits;
    }

    public void setMemcacheHits(int memcacheHits) {
        this.memcacheHits = memcacheHits;
    }

    public long getMemcacheTime() {
        return memcacheTime;
    }

    public void setMemcacheTime(long memcacheTime) {
        this.memcacheTime = memcacheTime;
    }

    public int getPersistentStoreHits() {
        return persistentStoreHits;
    }

    public void setPersistentStoreHits(int persistentStoreHits) {
        this.persistentStoreHits = persistentStoreHits;
    }

    public long getIndexTime() {
        return indexTime;
    }

    public void setIndexTime(long indexTime) {
        this.indexTime = indexTime;
    }

    public long getRecordLogTime() {
        return recordLogTime;
    }

    public void setRecordLogTime(long recordLogTime) {
        this.recordLogTime = recordLogTime;
    }

    public int getMisses() {
        return misses;
    }

    public void setMisses(int misses) {
        this.misses = misses;
    }

    public int getRecordLogReadErrors() {
        return recordLogReadErrors;
    }

    public void setRecordLogReadErrors(final int recordLogReadErrors) {
        this.recordLogReadErrors = recordLogReadErrors;
    }

    public int getIndexReadErrors() {
        return indexReadErrors;
    }

    public void setIndexReadErrors(final int indexReadErrors) {
        this.indexReadErrors = indexReadErrors;
    }

    public void clear() {
        memcacheHits = 0;
        memcacheTime = 0;
        persistentStoreHits = 0;
        indexTime = 0;
        recordLogTime = 0;
        misses = 0;
        recordLogReadErrors = 0;
        indexReadErrors = 0;
    }

    @Override
    public String toString() {
        return "CacheStats{" +
                "memcacheHits=" +
                memcacheHits +
                ", memcacheTime=" +
                memcacheTime +
                ", persistentStoreHits=" + persistentStoreHits +
                ", indexTime=" +
                indexTime +
                ", recordLogTime=" +
                recordLogTime +
                ", misses=" +
                misses +
                ", recordLogReadErrors=" +
                recordLogReadErrors +
                ", indexReadErrors=" +
                indexReadErrors +
                '}';
    }
}
