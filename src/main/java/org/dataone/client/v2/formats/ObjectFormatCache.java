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
 * 
 * $Id$
 */

package org.dataone.client.v2.formats;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.dataone.client.v2.CNode;
import org.dataone.client.v2.itk.D1Client;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v2.ObjectFormat;
import org.dataone.service.types.v2.ObjectFormatList;
import org.dataone.service.types.v2.util.ObjectFormatServiceImpl;
//import org.apache.oro.util.Cache;

/**
 * The ObjectFormatCache is a wrapper class for the DataONE ObjectFormatList
 * type.  It loads the most current object format list from a Coordinating Node,
 * and falls back to a default, albeit out of date, ObjectFormatList shipped with
 * the d1_libclient_java package.  The class provides accessor methods to query 
 * and manipulate the object format list.
 * 
 * To load an object format list other than the one configured for your environment,
 * set the property "ObjectFormatCache.overriding.CN_URL" in your application's 
 * configuration.  This is mainly used for integration and pre-registration testing.
 * 
 * @author cjones
 * @author rnahf
 *
 */
public class ObjectFormatCache {

	/* The instance of the logging class */
	private static Logger logger = Logger.getLogger(ObjectFormatCache.class.getName());

	/* The singleton instance */
	private static ObjectFormatCache objectFormatCache;
	
	
	/* refreshable raw cached information */ 
    private ObjectFormatList objectFormatList;
    
    /* refreshable cached information, derived from objectFormatList */ 
	private ConcurrentHashMap<ObjectFormatIdentifier,ObjectFormat>  objectFormatMap = new ConcurrentHashMap<>();
	
	

	/* flag for indicating whether we are still in a fallback situation */
	public static boolean usingFallbackFormatList = true;
	
	protected static int throttleIntervalSec = 20; 
	protected Date lastRefreshDate = new Date(0);

	/**
	 * Constructor: Creates an instance of the object format service using the
	 * the given Coordinating Node URL to load the authoritative object format
	 * list.
	 * 
	 * @param cnURL - the HTTP URL to the Coordinating Node to query
	 * @throws RuntimeException - if it fails to connect to a CN and fails to
	 * find the ObjectFormatList shipped with d1_libclient_java
	 */
	private ObjectFormatCache() {
	
		 throttleIntervalSec = Settings.getConfiguration()
		 	.getInt("ObjectFormatCache.minimum.refresh.interval.seconds",throttleIntervalSec);
		 
		 // populate the instance properties (the map and OFList)
		 try {
			 refreshCache();
		 } 
		 catch (ServiceFailure e) {
			 logger.error("Failed to get a ObjectFormatList from the CN or the default one" +
			 		" shipped with d1_libclient_java package. Cause = ServiceFailure::" + 
			         e.getDetail_code() + ": " + e.getDescription(), e);
			 RuntimeException re = new RuntimeException("Serious problem populating the ObjectFormatCache. Halting.");
			 re.initCause(e);
			 throw re;
		} 
	}


	private static class ObjectFormatCacheSingleton {	    
	    public final static ObjectFormatCache instance = new ObjectFormatCache();
	}

	/**
	 * Create the object format cache instance if it hasn't already been created.
	 */
	public static ObjectFormatCache getInstance() {
	    return ObjectFormatCacheSingleton.instance;
	}

	
	/**
	 * List the object formats registered with the object format service.
	 * 
	 * @return objectFormatList - the list of object formats
	 */
	public ObjectFormatList listFormats() {
		if (usingFallbackFormatList) {
			try {
				refreshCache();
			} catch (ServiceFailure e) {
				// do nothing
			}
		}
		return objectFormatList;
	}

	
	/**
	 * Returns the minimal refresh interval (seconds)
	 */
	public int getMinimalRefreshInterval() {
		return throttleIntervalSec;
	}
	
	
	/**
	 * Returns the date of the last refresh from the CN.
	 * Returns "Date zero" (Jan 1, 1970) if never refreshed from the CN
	 */
	public Date getLastRefreshDate() {
		return lastRefreshDate;
	}

	
	/**
	 * Returns true if the cache has not been able to successfully
	 * refresh from the CN
	 */
	public boolean isUsingFallbackFormatList() {
		return usingFallbackFormatList;
	}
	
	/**
	 * refreshes the cache from the CN or if a CN copy cannot be obtains, temporarily
	 * uses a static ObjectFormatList included in the libclient_java jar, accessed via
	 * org.dataone.service.types.v2.util.ObjectFormatServiceImpl.  Once there has been
	 * a successful load from the CN, the cache expires after a configurable number
	 * of seconds.
	 * 
	 * The refresh clears and replaces the previous cached ObjectFormatList.
	 * @throws ServiceFailure
	 */
    protected synchronized void refreshCache() throws ServiceFailure {
        // synchronizing avoids double/triple-refreshing and potential resource
        // contention.  The second caller piggybacks on the refresh work initiated
        // by the first, waiting for the refresh to happen, then simply doing a 
        // date comparison and returning.
        Date now = new Date();
        ObjectFormatList newObjectFormatList = null;

        logger.info("entering refreshCache()...");
        
        if ( usingFallbackFormatList  /* we should try to get the CN list */
               || now.getTime() - lastRefreshDate.getTime() > throttleIntervalSec * 1000)
        {
            CNode cn = null;
            String cnUrl = Settings.getConfiguration().getString("ObjectFormatCache.overriding.CN_URL");
            if (StringUtils.isBlank(cnUrl))
                cnUrl = Settings.getConfiguration().getString("D1Client.CN_URL");

            try {  // try to get an ObjectFormatList
                if (StringUtils.isBlank(cnUrl)) {
                    throw new ServiceFailure("0-client-side","Null D1Client.CN_URL: " + cnUrl);
                }
                cn = D1Client.getCN(cnUrl);
                logger.info("refreshing objectFormatCache from cn: " + cn.getNodeId());

                newObjectFormatList = cn.listFormats();
                usingFallbackFormatList = false;
                lastRefreshDate = new Date();

            } catch (ServiceFailure | NotImplemented e) {
                logger.warn("Could not refresh ObjectFormat cache from CN: " + cnUrl);
                
                if (usingFallbackFormatList) {
                    logger.warn("Will temporarily use the locally cached list.");
                    try {
//  For testing:                        throw new ServiceFailure("","");
                        newObjectFormatList = ObjectFormatServiceImpl.getInstance().listFormats();
                    } catch (ServiceFailure e1) {
                        logger.error("Could not get the local ObjectFormatList file shipped with the jar!!");
                        throw e1;
                    }
                } else {
                    // already tried to refresh, and don't want to go back to
                    // the local list.
                    logger.warn("Using stale objectFormatList...");
                }
            }

            if (newObjectFormatList != null) {
                if (getObjectFormatMap() != null) getObjectFormatMap().clear();
                for (ObjectFormat objectFormat : newObjectFormatList.getObjectFormatList())
                    getObjectFormatMap().put(objectFormat.getFormatId(), objectFormat);
                
                this.objectFormatList = newObjectFormatList;

                if (usingFallbackFormatList) 
                    logger.info("refreshed cache from format list shipped with libclient_java.");
                else 
                    logger.info("successfully refreshed cache from cn.listFormats()");
            }

        } else {
            logger.info("cache is still fresh. exiting without refresh.");
        }
    }





	/**
	 * Get the object format based on the given identifier string.
	 * <p>
	 * This method is deprecated in favor of the type-safe getFormat(ObjectFormatIdentifier)
	 * method
	 * 
	 * @param format - the object format identifier string
	 * @return objectFormat - the ObjectFormat represented by the format identifier
	 * @throws NotFound 
	 */
	@Deprecated
	public ObjectFormat getFormat(String fmtidStr) 
	throws NotFound 
	{   
		ObjectFormatIdentifier formatId = new ObjectFormatIdentifier();
		formatId.setValue(fmtidStr);
		return getFormat(formatId);

	}

	/**
	 * Lookup and return the format specified by the Identifier.
	 * If cache is stale or ID not in the cache, will refresh and try (again).
	 * returns the formatId or exception
	 * 
	 * @param formatId - the object format identifier
	 * @return objectFormat - the ObjectFormat represented by the format identifier
	 * 
	 * @throws ServiceFailure 
	 * @throws NotFound 
	 * @throws NotImplemented 
	 */
	public ObjectFormat getFormat(ObjectFormatIdentifier formatId)
	throws NotFound
	{ 	
		ObjectFormat objectFormat = 
			usingFallbackFormatList ?  null : getObjectFormatMap().get(formatId); 

		if ( objectFormat == null ) {
			try {
				refreshCache();
			} catch (ServiceFailure e) {
				// let it fail, so it can use the fallback
			}
			objectFormat = getObjectFormatMap().get(formatId);
		}
		
		if ( objectFormat == null ) {
			throw new NotFound("0000", "The format specified by " + formatId.getValue() + 
			" was not found after refreshing the cache.");
		}

		return objectFormat;
	}

	
	protected ConcurrentHashMap<ObjectFormatIdentifier, ObjectFormat> getObjectFormatMap() {
	    
	    return objectFormatMap;
	  }

}
