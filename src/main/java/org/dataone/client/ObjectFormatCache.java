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
  private static ObjectFormatCache objectFormatCache;
  
  /* The list of object formats */
  private ObjectFormatList objectFormatList;
  
  /* The searchable map of object formats */
  private HashMap<String, ObjectFormat> objectFormatMap;
  
  /* The fallback object format list from the disk cache */
  private ObjectFormatDiskCache objectFormatDiskCache;
  
  /**
   * Constructor: Creates an instance of the object format service using the
   * the given Coordinating Node URL to load the authoritative object format
   * list.
   * 
   * @param cnURL - the HTTP URL to the Coordinating Node to query
   */
  private ObjectFormatCache() {
        
  	try {
      
  	  // refresh the object format list
      populateObjectFormatList();
	
  	} catch (ServiceFailure sf ) {
  	  
  		logger.error("There was a problem creating the ObjectFormatCache. " +
  				        "The error message was: " + sf.getMessage());
  		// TODO: how should this be propagated since it is essentially fatal?
  	}
           
  }
  
  /*
   * Create the object format cache instance if it hasn't already been created.
   */
  public synchronized static ObjectFormatCache getInstance() {
    
  	if ( objectFormatCache == null ) {
  		objectFormatCache = new ObjectFormatCache();
  		
  	}
  	
  	return objectFormatCache;
  }
  
  /*
   * Return the hash containing the fmtid and format mapping
   * 
   * @return objectFormatMap - the hash of fmtid/format pairs
   */
  private HashMap<String, ObjectFormat> getObjectFormatMap() {
  	
  	if ( objectFormatMap == null ) {
  		objectFormatMap = new HashMap<String, ObjectFormat>();
  		
  	}
  	return objectFormatMap;
  	
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
  private  void populateObjectFormatList()
    throws ServiceFailure {
    
  	// get the backup cache instance if needed
  	objectFormatDiskCache = new ObjectFormatDiskCache();

    try {
  		
			// create the searchable map of object formats
      objectFormatList = this.getListFromCN();
      
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
      String identifier = objectFormat.getFmtid().getValue();
      getObjectFormatMap().put(identifier, objectFormat);
      
    }
    
  }
  
  /**
   * 
   * @return objectFormatList - the list of object formats
   * @throws ServiceFailure
   */
  private  ObjectFormatList getListFromCN() 
    throws InvalidRequest, ServiceFailure, NotFound, InsufficientResources,
    NotImplemented {
	  
  	try {
	    
    	// Get the reference to the CN
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
    
		return getInstance().objectFormatList;
    
  }
  
  /**
   * Get the object format based on the given identifier.
   * 
   * @param format - the object format identifier
   * @return objectFormat - the ObjectFormat represented by the format identifier
   * @throws NotFound 
   */
  public static ObjectFormat getFormat(ObjectFormatIdentifier fmtid) 
    throws NotFound {
    
    return getFormat(fmtid.toString());
            
  }

  /**
   * Get the object format based on the given identifier string.
   * 
   * @param format - the object format identifier string
   * @return objectFormat - the ObjectFormat represented by the format identifier
   * @throws NotFound 
   */
  public static ObjectFormat getFormat(String fmtidStr) 
    throws NotFound {        	
    
  	ObjectFormat objectFormat = null;
  	
    objectFormat = getInstance().getObjectFormatMap().get(fmtidStr);
    
    if ( objectFormat == null ) {
      
    	throw new NotFound("4848", "The format specified by " + fmtidStr + 
    			               "does not exist at this node.");
    }
    
    return objectFormat;
    
  }
  
  
}
