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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;
import org.dataone.client.exception.NotCached;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.SystemMetadata;

/**
 * LocalCache creates a local store of objects that can be locally accessed without
 * incurring the overhead of remote service calls.  Two types of caches are created,
 * one for immutable DataONE objects (data, science metadata, and ORE packages), and
 * one for mutable SystemMetadata objects.  Each is accessed using different get/put methods.
 * The singleton is created on first use of the instance() method, which initializes 
 * the local caches.  LocalCache uses both an in-memory LRU cache for frequently accessed
 * objects, and a longer-lived disk-based auxiliary cache for objects that have been 
 * expired from the in-memory cache.  The configuration of these caches, their expiration 
 * policies, and disk locations are determined from the cache.ccf properties file.
 */
public class LocalCache {

    private JCS dataCache = null;
    private JCS sysmetaCache = null;

    private static LocalCache localCache = null;
    private int hits = 0;
    private int misses = 0;
    
    private static Log log = LogFactory.getLog(LocalCache.class);

    /**
     * Create the single instance of the LocalCache using a private constructor.
     */
    private LocalCache() {
        super();
        try {
            this.dataCache = JCS.getInstance("DATA_CACHE");
            this.sysmetaCache = JCS.getInstance("SYSMETA_CACHE");
        } catch (CacheException e) {
            log.error("Error while creating LocalCache. " + e.getMessage());
        }
    }
    
    /**
     * Get the singleton instance of the LocalCache that can be used to perform
     * cache operations.
     * @return the LocalCache instance
     */
    public static LocalCache instance() {
        synchronized (LocalCache.class) {
            if (null == localCache) {
                localCache = new LocalCache();
            }
        }
        return localCache;
    }

    /**
     * Put an immutable data object into the local cache by serializing it as
     * a byte array, indexed by its unique Identifier.
     * @param key the Identifier used to index the object
     * @param data to be cached
     */
    public void putData(Identifier key, byte[] data) {
        try {
            dataCache.put(key, data);
        } catch (CacheException e) {
            log.error("Error while putting data in LocalCache. " + e.getMessage());
        }
    }
    
    /**
     * Retrieve an array of bytes from the cache, throwing NotCached if the object
     * with the Identifier key is not in the cache.
     * @param key the Identifier of the cached object to retrieve
     * @return the array of bytes of the object
     * @throws NotCached
     */
    public byte[] getData(Identifier key) throws NotCached {
        byte[] cachedData = (byte[])dataCache.get(key);
        if (cachedData == null) {
            misses++;
            throw new NotCached("Object not cached: " + key.getValue());
        } else {
            hits++;
        }
        return cachedData;
    }
    
    /**
     * Put a mutable SystemMetadata object into the local cache, indexed by the 
     * unique Identifier of the object that this system metadata describes. Because
     * SystemMetadata is mutable, care should be taken to only use the cache when
     * the calling application can be sure that it is safe to do so.
     * @param key the Identifier used to index the object
     * @param SystemMetadata to be cached
     */
    public void putSystemMetadata(Identifier key, SystemMetadata sysmeta) {
        try {
            sysmetaCache.put(key, sysmeta);
        } catch (CacheException e) {
            log.error("Error while putting system metadata in LocalCache. " + e.getMessage());
        }
    }
    
    /**
     * Retrieve a SystemMetadata from the cache, throwing NotCached if the system metadata
     * for the Identifier key is not in the cache. Because
     * SystemMetadata is mutable, care should be taken to only use the cache when
     * the calling application can be sure that it is safe to do so.
     * @param key the Identifier of the cached SystemMetadata to retrieve
     * @return the SystemMetadata of the object
     * @throws NotCached
     */
    public SystemMetadata getSystemMetadata(Identifier key) throws NotCached {
        SystemMetadata sm = (SystemMetadata)sysmetaCache.get(key);
        if (sm == null) {
            misses++;
            throw new NotCached("Object not cached: " + key.getValue());
        } else {
            hits++;
        }
        return sm;
    }
    
    /**
     * Returns the cache hits, the number of times that objects are requested 
     * and found in the cache since the last time that counters were reset.
     * @return the hits
     */
    public int getHits() {
        return hits;
    }

    /**
     * Returns the cache misses, the number of times that objects are requested 
     * but are not found in the cache since the last time that counters were reset.
     * @return the misses
     */
    public int getMisses() {
        return misses;
    }
    
    /**
     * Reset the cache hit/miss counters to 0.
     */
    public void resetCounters() {
        this.hits = 0;
        this.misses = 0;
    }
}
