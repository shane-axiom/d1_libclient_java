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

import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.dataone.service.ObjectFormatDiskCache;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.ObjectFormat;
import org.dataone.service.types.ObjectFormatIdentifier;
import org.dataone.service.types.ObjectFormatList;

/**
 * The ObjectFormatCache is a wrapper class for the DataONE ObjectFormatList
 * type.  It loads the most current object format list from a Coordinating Node,
 * or loads an on-disk cache version as a fall back. It provides accessor
 * methods to query and manipulate the object format list.
 * 
 * @author cjones
 *
 */
public class ObjectFormatCache {
  
  /* The instance of the logging class */
  private static Logger logger = Logger.getLogger(ObjectFormatCache.class.getName());
  
  /* The instance of the object format cache */
  private ObjectFormatCache objectFormatCache = null;
  
  /* The list of object formats */
  private static ObjectFormatList objectFormatList = null;
  
  /* The searchable map of object formats */
  private static TreeMap<ObjectFormatIdentifier, ObjectFormat> objectFormatMap = 
    new TreeMap<ObjectFormatIdentifier, ObjectFormat>();
  
  /* The fallback object format list from the disk cache */
  private static ObjectFormatDiskCache objectFormatDiskCache = null;
  
  /* The D1 Coordinating Node URL used for object format lookups */
  private static String coordinatingNodeURL = null;

  /**
   * Constructor: Creates an instance of the object format service using the
   * the given Coordinating Node URL to load the authoritative object format
   * list.
   * 
   * @param cnURL - the HTTP URL to the Coordinating Node to query
   */
  public ObjectFormatCache(String cnURL) {
        
  	try {
      
  		this.coordinatingNodeURL = cnURL;
  	  // refresh the object format list
  		doRefresh();
  	
  	} catch (ServiceFailure sf ) {
  	  
  		logger.error("There was a problem creating the ObjectFormatCache. " +
  				        "The error message was: " +sf.getMessage());
  		// TODO: how should this be propagated since it is essentially fatal?
  	}
           
  }

  /**
   * Updates the object format cache re-querying the CN
   */
  public static void doRefresh() throws ServiceFailure {
    
    // refresh the list of object formats
    try {
    	
	    populateObjectFormatList();
	    
    } catch (ServiceFailure sf) {
	    
    	throw sf;
	    
    }
    
    return;
  }
    
  /**
   * Populate the ObjectFormatCache's objectFormatList from the cached list.
   * 
   * @throws ServiceFailure
   * @throws NotImplemented 
   * @throws InsufficientResources 
   * @throws NotFound 
   * @throws InvalidRequest 
   */
  private static void populateObjectFormatList() 
    throws ServiceFailure {
    
  	// get the backup cache instance if needed
  	objectFormatDiskCache = new ObjectFormatDiskCache();

    try {
  		
    	// create the searchable map of object formats
      objectFormatList = getListFromCN();
      
    } catch (ServiceFailure se) {

  		logger.error("There was a problem creating the ObjectFormatCache. "    +
	        "Reverting to the on-disk cache version.  The error message was: " + 
	        se.getMessage());
      objectFormatList = objectFormatDiskCache.listFormats();
        
    } catch ( NotFound nfe ) {
  	  
  		logger.error("There was a problem creating the ObjectFormatCache. "    +
	        "Reverting to the on-disk cache version.  The error message was: " + 
	        nfe.getMessage());
  		objectFormatList = objectFormatDiskCache.listFormats();

    } catch ( InvalidRequest ire ) {
      
  		logger.error("There was a problem creating the ObjectFormatCache. "    +
	        "Reverting to the on-disk cache version.  The error message was: " + 
	        ire.getMessage());
  		objectFormatList = objectFormatDiskCache.listFormats();

    } catch ( InsufficientResources isre ) {
      
  		logger.error("There was a problem creating the ObjectFormatCache. "    +
	        "Reverting to the on-disk cache version.  The error message was: " + 
	        isre.getMessage());
  		objectFormatList = objectFormatDiskCache.listFormats();

    } catch ( NotImplemented nie ) {
      
  		logger.error("There was a problem creating the ObjectFormatCache. "    +
	        "Reverting to the on-disk cache version.  The error message was: " + 
	        nie.getMessage());
  		objectFormatList = objectFormatDiskCache.listFormats();

    }

    // index the object format list based on the format identifier
    int listSize = objectFormatList.sizeObjectFormats();
    
    for (int i = 0; i < listSize; i++ ) {
      
      ObjectFormat objectFormat = 
        objectFormatList.getObjectFormat(i);
      ObjectFormatIdentifier identifier = objectFormat.getFmtid();
      objectFormatMap.put(identifier, objectFormat);
      
    }
    
  }
  
  /**
   * 
   * @return objectFormatList - the list of object formats
   * @throws ServiceFailure
   */
  private static ObjectFormatList getListFromCN() 
    throws InvalidRequest, ServiceFailure, NotFound, InsufficientResources,
    NotImplemented {
	  
  	try {
	    
    	// Get the reference to the CN
  		D1Client d1Client = new D1Client(coordinatingNodeURL);
	    CNode cn = D1Client.getCN();
	    
	    // get the object format list from the CN
	    objectFormatList = cn.listFormats();
    
  	} catch (ServiceFailure e) {
    	throw e;
    	
    }
  	
		return objectFormatList;

  }

  /**
   * List the object formats registered with the object format service.
   * 
   * @return objectFormatList - the list of object formats
   */
  public static ObjectFormatList listFormats() {
    
    return objectFormatList;
    
  }
  
  /**
   * Get the object format based on the given identifier.
   * 
   * @param format - the object format identifier
   * @return objectFormat - the ObjectFormat represented by the format identifier
   */
  public static ObjectFormat getFormat(ObjectFormatIdentifier fmtid) {
    
    ObjectFormat objectFormat = null;
    objectFormat = objectFormatMap.get(fmtid);
    
    // ensure the list is up to date
    if ( objectFormat == null ) {
      
    	try {
	      doRefresh();

    	} catch (ServiceFailure sf) {
    		// do not throw ServiceFailure, but rather return a null format
        logger.debug("The format specified by " + fmtid.getValue() +
        		"was not found in the object format map.");
    	}	
      
    }
    
    // try again with the refreshed map, may still return null
    objectFormat = objectFormatMap.get(fmtid);

    return objectFormat;
    
  }

  /**
   * Get the object format based on the given identifier string.
   * 
   * @param format - the object format identifier string
   * @return objectFormat - the ObjectFormat represented by the format identifier
   */
  public static ObjectFormat getFormat(String fmtidStr) {
    
    ObjectFormat objectFormat = null;
    ObjectFormatIdentifier fmtid = new ObjectFormatIdentifier();
    fmtid.setValue(fmtidStr);
    objectFormat = getFormat(fmtid);
      
    return objectFormat;
    
  }
  
  
}
