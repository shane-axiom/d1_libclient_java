/**
  * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dataone.client.cache;

import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.exception.NotCached;
import org.dataone.client.v1.cache.LocalCache;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.SystemMetadata;
import org.junit.Test;

/**
 * Test the intialization of a LocalCache, along with the timing of putting and
 * getting a set of objects and SystemMetadata from that local cache in comparison
 * to a simulated, expensive remote service call.
 */
public class LocalCacheTest {

    private int objects = 10;
    private static final int SIZE = 100;
    private static final String PREFIX = "KEY";
    private static Log log = LogFactory.getLog(LocalCacheTest.class);
    LocalCache localCache = null;
    
    /**
     * Test the data cache by timing the period needed to create a set of objects,
     * then compare this time to the time taken when the objects are cached.
     */
    @Test
    public void testDataCache() {
        localCache = LocalCache.instance();
        
        localCache.resetCounters();
        long t0 = serviceBaseline(localCache);
        assertTrue(t0 > 0);
        
        localCache.resetCounters();
        long t1 = cacheTimeTest(localCache);
        assertTrue(t1 > 0);
        
        localCache.resetCounters();
        long t2 = cacheTimeTest(localCache);
        assertTrue(t2 < t0);
        
        localCache.resetCounters();
        long t3 = cacheTimeTest(localCache);
        assertTrue(t3 < t0);
        
    }

    /**
     * Test the system metadata cache by timing the period needed to create a set of objects,
     * then compare this time to the time taken when the objects are cached.
     */
    @Test
    public void testSysmetaCache() {
        localCache = LocalCache.instance();
        
        localCache.resetCounters();
        long t0 = sysmetaBaseline(localCache);
        assertTrue(t0 > 0);
        
        localCache.resetCounters();
        long t1 = sysmetaTimeTest(localCache);
        assertTrue(t1 > 0);
        
        localCache.resetCounters();
        long t2 = sysmetaTimeTest(localCache);
        assertTrue(t2 < t0);
        
        localCache.resetCounters();
        long t3 = sysmetaTimeTest(localCache);
        assertTrue(t3 < t0);
        
    }
    
    /**
     * Time the creation of a set of objects without any cacheing.
     */
    private long serviceBaseline(LocalCache localCache) {
        byte[] cachedData = null;
        final long startTime = System.nanoTime();
        final long endTime;
        
        // First time using the cache
        try {
            for (int key = 0; key < objects; key++) {
                String keystring = PREFIX + key;
                Identifier id = new Identifier();
                id.setValue(keystring);
                cachedData = getFromService(id);
            }
        } finally {
          endTime = System.nanoTime();
        }
        final long duration = (endTime - startTime)/(1000*1000);
        log.debug("Value: " + cachedData + " Duration is: " + duration + " ms (" + localCache.getHits() + "/" + localCache.getMisses() + ")");
        return duration;
    }
    
    /**
     * Time the creation of a set of objects using the local cache.
     */
    private long cacheTimeTest(LocalCache localCache) {
        byte[] cachedData = null;
        final long startTime = System.nanoTime();
        final long endTime;
        
        try {
            for (int key = 0; key < objects; key++) {
                String keystring = PREFIX + key;
                Identifier id = new Identifier();
                id.setValue(keystring);
                try {
                    cachedData = localCache.getData(id);
                } catch (NotCached e) {
                    byte[] data = getFromService(id);
                    localCache.putData(id, data);
                    cachedData = data;
                }
            }
        } finally {
          endTime = System.nanoTime();
        }
        final long duration = (endTime - startTime)/(1000*1000);
        
        log.debug("Value: " + cachedData + " Duration is: " + duration + " ms (" + localCache.getHits() + "/" + localCache.getMisses() + ")");
        return duration;
    }
    
    /**
     * Simulate a remote service that would be used to get objects that could be cached locally.
     */
    private byte[] getFromService(Identifier key) {
        byte[] data = new byte[SIZE];
        Random rng = new Random();
        rng.nextBytes(data);
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return data;
    }
    
    /**
     * Time the creation of a set of system metadata without any cacheing.
     */
    private long sysmetaBaseline(LocalCache localCache) {
        SystemMetadata sm = null;
        final long startTime = System.nanoTime();
        final long endTime;
        
        // First time using the cache
        try {
            for (int key = 0; key < objects; key++) {
                String keystring = PREFIX + key;
                Identifier id = new Identifier();
                id.setValue(keystring);
                sm = getSysmetaFromService(id);
            }
        } finally {
          endTime = System.nanoTime();
        }
        final long duration = (endTime - startTime)/(1000*1000);
        log.debug("Value: " + sm + " Duration is: " + duration + " ms (" + localCache.getHits() + "/" + localCache.getMisses() + ")");
        return duration;
    }
    
    /**
     * Time the creation of a set of system metadata using the local cache.
     */
    private long sysmetaTimeTest(LocalCache localCache) {
        SystemMetadata sm = null;
        final long startTime = System.nanoTime();
        final long endTime;
        
        try {
            for (int key = 0; key < objects; key++) {
                String keystring = PREFIX + key;
                Identifier id = new Identifier();
                id.setValue(keystring);
                try {
                    sm = localCache.getSystemMetadata(id);
                } catch (NotCached e) {
                    SystemMetadata newSysmeta = getSysmetaFromService(id);
                    localCache.putSystemMetadata(id, newSysmeta);
                    sm = newSysmeta;
                }
            }
        } finally {
          endTime = System.nanoTime();
        }
        final long duration = (endTime - startTime)/(1000*1000);
        
        log.debug("Value: " + sm + " Duration is: " + duration + " ms (" + localCache.getHits() + "/" + localCache.getMisses() + ")");
        return duration;
    }
    
    /**
     * Simulate a remote service that would be used to get SystemMetadata that could be cached locally.
     */
    private SystemMetadata getSysmetaFromService(Identifier key) {
        SystemMetadata sm = new SystemMetadata();
        sm.setIdentifier(key);
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return sm;
    }
    
}
