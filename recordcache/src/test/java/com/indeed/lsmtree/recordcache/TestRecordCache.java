package com.indeed.lsmtree.recordcache;

import junit.framework.TestCase;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class TestRecordCache extends TestCase {

    private static final Logger log = Logger.getLogger(TestRecordCache.class);

    public void testNothing() {
        assertNotNull(new CacheStats().toString());
    }
}
