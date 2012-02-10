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

package org.dataone.client;

import java.util.HashMap;

import org.apache.log4j.Logger;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.util.ObjectFormatServiceImpl;
import org.dataone.service.types.v1.ObjectFormat;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ObjectFormatList;

/**
 * The ObjectFormatCache is a wrapper class for the DataONE ObjectFormatList
 * type.  It loads the most current object format list from a Coordinating Node,
 * or loads an on-disk cache version as a fall back. It provides accessor
 * methods to query and manipulate the object format list.
 * 
 * @author cjones
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
	public static boolean isUpdatedFromCN = false;

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

		 // update the cache with any new information from this class.
		 try {
			 refreshCache();
		 } 
		 /*  need to swallow exceptions from call to CN so that
		  * there's an ObjectFormatCache instance that can access
		  * the fallback cached objectFormatList
		  */
		 catch (ServiceFailure e) {
			 // TODO: any secondary decisions to make regarding cache refresh frequency
		} catch (NotImplemented e) {
			 // TODO: any secondary decisions to make regarding cache refresh frequency
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
		if (isUpdatedFromCN) {
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

		
//	/*
//	 * Return the hash containing the formatId and format mapping
//	 * 
//	 * @return objectFormatMap - the hash of formatId/format pairs
//	 */
//	private HashMap<ObjectFormatIdentifier, ObjectFormat> getObjectFormatMap() {
//
//		if ( objectFormatMap == null ) {
//			objectFormatMap = new HashMap<ObjectFormatIdentifier, ObjectFormat>();
//
//		}
//		return objectFormatMap;
//
//	}

	/**
	 * 
	 * @return objectFormatList - the list of object formats
	 * @throws ServiceFailure
	 * @throws NotImplemented
	 */
	private synchronized ObjectFormatList refreshCache() throws ServiceFailure, NotImplemented {

		// TODO: do we need/wish to make sure the returned list is longer, or "more complete"
		// than the existing one before replacing?  (specifically the one on file in the jar)
		// what would be the criteria?  
		ObjectFormatList objectFormatList = D1Client.getCN().listFormats();

		// index the object format list by the format identifier
		for (ObjectFormat objectFormat : objectFormatList.getObjectFormatList())
		{
			getObjectFormatMap().put(objectFormat.getFormatId(), objectFormat);
		}
		this.isUpdatedFromCN = true;

		return objectFormatList;
	}





	/**
	 * Get the object format based on the given identifier string.
	 * 
	 * @param format - the object format identifier string
	 * @return objectFormat - the ObjectFormat represented by the format identifier
	 * @throws NotFound 
	 */
	public ObjectFormat getFormat(String fmtidStr) 
	throws NotFound {   
		ObjectFormatIdentifier formatId = new ObjectFormatIdentifier();
		formatId.setValue(fmtidStr);
		try {
			return getFormat(formatId);
		} catch (ServiceFailure e) {
			throw new NotFound(e.getDetail_code(),e.getMessage());
		} catch (NotImplemented e) {
			throw new NotFound(e.getDetail_code(),e.getMessage());
		}

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
	throws NotFound, ServiceFailure, NotImplemented
	{ 	
		ObjectFormat objectFormat = 
			isUpdatedFromCN ?  getObjectFormatMap().get(formatId)  :  null; 

		if ( objectFormat == null ) {
			refreshCache();
			objectFormat = getObjectFormatMap().get(formatId);
		}
		
		if ( objectFormat == null ) {
			throw new NotFound("0000", "The format specified by " + formatId.getValue() + 
			" was not found after refreshing the cache.");
		}

		return objectFormat;
	}


}
