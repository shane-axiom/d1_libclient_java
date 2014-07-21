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

package org.dataone.client.v1.formats;

import java.util.Date;

import org.apache.log4j.Logger;
import org.dataone.client.v1.CNode;
import org.dataone.client.v1.impl.MultipartCNode;
import org.dataone.client.v1.itk.D1Client;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.ObjectFormat;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ObjectFormatList;
import org.dataone.service.types.v1.util.ObjectFormatServiceImpl;

/**
 * The ObjectFormatCache is a wrapper class for the DataONE ObjectFormatList
 * type.  It loads the most current object format list from a Coordinating Node,
 * and falls back to on-disk cache version.  It provides accessor
 * methods to query and manipulate the object format list.
 * 
 * To load an object format list other than the one configured for your environment,
 * set the property "ObjectFormatCache.overriding.CN_URL" in your application's 
 * configuration.  This is mainly used for integration and pre-registration testing.
 * 
 * @author cjones
 * @author rnahf
 *
 */
public class ObjectFormatCache extends ObjectFormatServiceImpl {

	/* The instance of the logging class */
	private static Logger logger = Logger.getLogger(ObjectFormatCache.class.getName());

	/* The instance of the object format cache */
	private static ObjectFormatCache objectFormatCache;
	
	/* 
	 * a boolean to indicate whether successfully reached the CN yet, or still in a
	 *  bootstrap situation
	 */
	public static boolean usingFallbackFormatList = true;

	protected static int throttleIntervalSec = 20; 
	protected Date lastRefreshDate;
	/* The list of object formats */
	//  private ObjectFormatList objectFormatList;

	/* The searchable map of object formats */
	//  private HashMap<ObjectFormatIdentifier, ObjectFormat> objectFormatMap;

	/**
	 * Constructor: Creates an instance of the object format service using the
	 * the given Coordinating Node URL to load the authoritative object format
	 * list.
	 * 
	 * @param cnURL - the HTTP URL to the Coordinating Node to query
	 * @throws ServiceFailure 
	 * @throws NotImplemented 
	 */
	private ObjectFormatCache() throws ServiceFailure {

		 super();  // populates the default cache shipped with super's jar.

		 // set the last cache refresh date to a long time ago
//		 lastRefreshDate = new Date(0);
		 
		 throttleIntervalSec = Settings.getConfiguration()
		 	.getInt("ObjectFormatCache.minimum.refresh.interval.seconds",throttleIntervalSec);
		 
		 // update the cache with any new information from this class.
		 try {
			 refreshCache();
		 } 
		 /*  need to swallow exceptions from call to CN so that
		  * there's an ObjectFormatCache instance that can access
		  * the fallback cached objectFormatList
		  */
		 catch (ServiceFailure e) {
			// TODO: any secondary decisions to make regarding cache refresh frequency?
			 logger.warn("Failed to get current ObjectFormatList from the CN, using fallback" +
			 		"list provided with libclient. Cause = ServiceFailure::" + e.getDetail_code() + ": " +
			 				e.getDescription());
		} catch (NotImplemented e) {
			 // TODO: any secondary decisions to make regarding cache refresh frequency?
			 logger.warn("Failed to get current ObjectFormatList from the CN, using fallback" +
				 		"list provided with libclient. Cause = NotImplemented::" + e.getDetail_code() + ": " +
				 				e.getDescription());
		}
	}

	/**
	 * Create the object format cache instance if it hasn't already been created.
	 * 
	 * @throws ServiceFailure - upon problems creating the cache
	 */
	public synchronized static ObjectFormatCache getInstance() throws ServiceFailure {

		if ( objectFormatCache == null ) {
			objectFormatCache = new ObjectFormatCache();

		}
		return objectFormatCache;
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
			} catch (NotImplemented e) {
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
	 * If null, no successful refresh has occurred. 
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
	 * refreshes the cache from the CN, provided that the minimal interval
	 * has been reached or there has never been a successful refresh from 
	 * the CN.
	 * @return objectFormatList - the list of object formats
	 * @throws ServiceFailure
	 * @throws NotImplemented
	 */
	private synchronized ObjectFormatList refreshCache() 
	throws ServiceFailure, NotImplemented 
	{
		Date now = new Date();

		if ( usingFallbackFormatList  ||
				lastRefreshDate == null ||
				now.getTime() - lastRefreshDate.getTime() > throttleIntervalSec * 1000)
		{
			CNode cn = null;
			String overridingCN = Settings.getConfiguration().getString("ObjectFormatCache.overriding.CN_URL");
			if (overridingCN != null) {
				cn = new MultipartCNode(overridingCN, null);
			} else {
				cn = D1Client.getCN();
			}
			logger.info("refreshing objectFormatCache from cn: " + cn.getNodeId());
			// TODO: do we need/wish to make sure the returned list is longer, or "more complete"
			// than the existing one before replacing?  (specifically the one on file in the jar)
			// what would be the criteria? 
			
			ObjectFormatList objectFormatList = cn.listFormats();
			lastRefreshDate = new Date();
			// index the object format list by the format identifier
			for (ObjectFormat objectFormat : objectFormatList.getObjectFormatList())
			{
				getObjectFormatMap().put(objectFormat.getFormatId(), objectFormat);
			}
			usingFallbackFormatList = false;
			logger.info("successful cache refresh from cn.listFormats()");
		}
		return objectFormatList;
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
	@Override
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
			} catch (NotImplemented e) {
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


}
