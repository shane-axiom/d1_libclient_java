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

package org.dataone.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.dataone.ore.ResourceMapFactory;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.SystemMetadata;
import org.dspace.foresite.OREException;
import org.dspace.foresite.OREParserException;
import org.dspace.foresite.ORESerialiserException;
import org.dspace.foresite.ResourceMap;

/**
 * A DataPackage is a collection of interrelated data (science data, metadata) 
 * D1Objects plus their defined relationships (the ResourceMap object)
 * 
 * DataPackage allows all of the science metadata, data objects, and system metadata
 * associated with those objects to be accessed from a common place.  A well-formed
 * DataPackage contains one or more science metadata D1Objects that document
 * one or more data D1Objects.  The DataPackage relationship graph can be serialized 
 * as an OAI-ORE ResourceMap, which details the linkages among science data objects 
 * and science metadata objects.  
 * 
 * The ResourceMap is a first-class object in the DataONE object collection, the 
 * same as metadata and science data, having its own unique identifier. Retrieving 
 * a DataPackage from DataONE therefore involves first retrieving the ResourceMap, 
 * then retrieving the members individually. That is to say, the ResourceMap 
 * contains only references to the package members, not the content.
 * 
 * DataPackage instances independently maintain 2 properties, the object map, and the 
 * relationships map.  The object map maintains the local representations of the 
 * D1Objects associated with the DataPackage, and should not contain data objects 
 * not found in the relationship map.  It is also used internally to access the 
 * package's data members.
 * 
 * The relationships map determines the contents of the ResourceMap, so therefore
 * is the ultimate authority on package membership.
 * 
 * When creating (submitting) a new package, it is important for the object map
 * and relationship map to be consistent.  The getUnresolvableMembers() and 
 * getUncharacterizeddMembers() methods should be invoked before uploading package 
 * members and the resourceMap to a MemberNode.
 * 
 */
public class DataPackage {
    
    private Identifier packageId;
    private Map<Identifier, List<Identifier>> metadataMap;
    private HashMap<Identifier, D1Object> objectStore;
    private SystemMetadata systemMetadata = null;
    
        
    /**
     * Default constructor without identifier.
     * Identifier should be provided later using setPackageId() method
     */
    public DataPackage() {
    	this(null);
    }
    
    /**
     * Construct a DataPackage using the given identifier to identify this package.  
     * The id is used as the identifier of the associated ORE map for this package.
     * @param id the Identifier of the package
     */
    public DataPackage(Identifier id) {
        objectStore = new HashMap<Identifier, D1Object>();
        metadataMap = new HashMap<Identifier, List<Identifier>>();
        setPackageId(id);
    }
    
    /**
     * Puts an object with the given Identifier to the package's local, temporary
     * data store. This creates a D1Object to wrap the identified object 
     * after downloading it from DataONE.
     * @param id the identifier of the object to be added
     * @throws InvalidRequest 
     * @throws InsufficientResources 
     * @throws NotImplemented 
     * @throws NotFound 
     * @throws NotAuthorized 
     * @throws ServiceFailure 
     * @throws InvalidToken 
     */
    public void addAndDownloadData(Identifier id) throws InvalidToken, ServiceFailure, 
    NotAuthorized, NotFound, NotImplemented, InsufficientResources, InvalidRequest 
    {
        if (!contains(id)) {
        	D1Object o = D1Object.download(id);
        	objectStore.put(id, o);
        }
    }
    
    /**
     * Puts a D1Object directly to the package's local, temporary data store without 
     * downloading it from a node.  The identifier for this object is extracted 
     * from the D1Object. 
     * @param obj the D1Object to be added to the object map
     */
    public void addData(D1Object obj) {
        Identifier id = obj.getIdentifier();
        if (!contains(id)) {
            if (obj != null) {
                objectStore.put(id, obj);
            }
        }
    }
    
    // TODO: consider renaming the method to include the relationship to be added to the 
    // resourceMap
    /**
     * Declare which data objects are documented by a metadata object, using their
     * identifiers.  Additional calls using the same metadata identifier will append 
     * to existing data identifier lists. Identifiers used in this call will de 
     * facto be part of any serialized resource map derived from the DataPackage 
     * instance.
     * 
     * @param metadataID
     * @param dataIDList
     */
    public void insertRelationship(Identifier metadataID, List<Identifier> dataIDList) {
        List<Identifier> associatedData = null;
        
        // Determine if the metadata object is already in the relations list
        // Use it if so, if not then create a list for this metadata link        	
        if (metadataMap.containsKey(metadataID)) {
            associatedData = metadataMap.get(metadataID);
        } else {
            associatedData = new ArrayList<Identifier>();
        }
        
        // For each data item, add the relationship if it doesn't exist
        for (Identifier dataId : dataIDList) {
            if (!associatedData.contains(dataId)) {
                associatedData.add(dataId);
            }
        }
        if (!metadataMap.containsKey(metadataID))
        	metadataMap.put(metadataID, associatedData);
    }
    
    /**
     * Used to introspect on the local temporary data store, NOT the number of 
     * DataPackage members.
     * @return the number of objects in the local temporary data store
     */
    //TODO: rename. size() is ambiguous and potentially inaccurate,
    //  as the number of objects in the objectStore is 
    // different than the number of package members.
    public int size() {
        return objectStore.size();
    }
    
    
    /**
     * Determine if an object with the given Identifier is already present in
     * the local data store.
     * @param id the Identifier to be checked
     * @return boolean true if the Identifier is in the package
     */
    public boolean contains(Identifier id) {
        return objectStore.containsKey(id);
    }
    
    /**
     * Get the D1Object associated with a given Identifier from the local data store.
     * @param id the identifier of the object to be retrieved
     * @return the D1Object for that identifier, or null if not found
     */
    public D1Object get(Identifier id) {
        return objectStore.get(id);
    }
    
    /**
     * @param id
     * @deprecated renaming to removeData(Identifier id) for naming consistency
     * with addData, addAndDownloadData
     */
    public void remove(Identifier id) {
    	removeData(id);
    }
    
    /**
     * Removes a D1Object from the local data store based on its Identifier.  Does
     * not affect the relationship map.
     * @param id the Identifier of the object to be removed.
     * @since v1.1.1
     */
    public void removeData(Identifier id) {
        objectStore.remove(id);
    }
    
    /**
     * Return the set of Identifiers that are part of this package.
     * @return a Set of Identifiers in the package
     */
    public Set<Identifier> identifiers() {
        return objectStore.keySet();
    }
    
    /**
     * @return the packageId
     */
    public Identifier getPackageId() {
        return packageId;
    }

    /**
     * @param packageId the packageId to set
     */
    public void setPackageId(Identifier packageId) {
        if (null != packageId) {
            this.packageId = packageId;
        }
    }

  
    /**
     * Build and return a fresh ORE ResourceMap from the metadata map.
     * TODO: create a RM when science metadata is null
     * TODO: handle error conditions when data list is null
     * @return the map
     * @throws URISyntaxException 
     * @throws OREException 
     */
    public ResourceMap getMap() throws OREException, URISyntaxException 
    {
    	return ResourceMapFactory.getInstance().createResourceMap(packageId, metadataMap);
    }
    
    
    
    /**
     * Return an ORE ResourceMap describing this package, serialized as an RDF graph.
     * @return the map as a serialized String
     * @throws URISyntaxException 
     * @throws OREException 
     * @throws ORESerialiserException 
     */
    public String serializePackage() throws OREException, URISyntaxException, ORESerialiserException 
    {
        ResourceMap rm = getMap();
        String  rdfXml = ResourceMapFactory.getInstance().serializeResourceMap(rm);
     
        return rdfXml;
    }
    
    /**
     * Download the resource map
     * @param pid
     * @return
     * @throws InvalidToken
     * @throws ServiceFailure
     * @throws NotAuthorized
     * @throws NotFound
     * @throws NotImplemented
     * @throws InsufficientResources
     * @throws InvalidRequest
     * @throws OREException
     * @throws URISyntaxException
     * @throws OREParserException
     * @throws IOException
     */
    public static DataPackage download(Identifier pid) 
    throws InvalidToken, ServiceFailure, NotAuthorized,
    NotFound, NotImplemented, InsufficientResources, InvalidRequest, OREException, 
    URISyntaxException, OREParserException, IOException
    {
    	D1Object packageObject = D1Object.download(pid);
    	
    	if (packageObject.getFormatId().getValue().equals("http://www.openarchives.org/ore/terms")) {
    		String resourceMap = IOUtils.toString(packageObject.getDataSource().getInputStream());
        	return deserializePackage(resourceMap);    		
    	}
    	throw new InvalidRequest("0000","The identifier does not represent a DataPackage (is not an ORE resource map)");
    }
    
    /**
     * Deserialize an ORE resourceMap by parsing it, extracting the associated package identifier,
     * and the list of metadata and data objects aggregated in the ORE Map.  Create an instance
     * of a DataPackage, and for each metadata and data object in the aggregation, add it to the
     * package.
     * @param resourceMap the string representation of an ORE map in XML format
     * @return DataPackage constructed from the map
     * @throws OREParserException 
     * @throws URISyntaxException 
     * @throws OREException 
     * @throws UnsupportedEncodingException 
     * @throws InvalidRequest 
     * @throws InsufficientResources 
     * @throws NotImplemented 
     * @throws NotFound 
     * @throws NotAuthorized 
     * @throws ServiceFailure 
     * @throws InvalidToken 
     */
    public static DataPackage deserializePackage(String resourceMap) 
    throws UnsupportedEncodingException, OREException, URISyntaxException, OREParserException, 
    InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented, InsufficientResources, 
    InvalidRequest 
    {
        
        Map<Identifier, Map<Identifier, List<Identifier>>> packageMap = 
        		ResourceMapFactory.getInstance().parseResourceMap(resourceMap);

        DataPackage dp = null;
        if (packageMap != null && !packageMap.isEmpty()) {

        	// Get and store the package Identifier in a new DataPackage
        	Identifier pid = packageMap.keySet().iterator().next();
        	dp = new DataPackage(pid);

        	// Get the Map of metadata/data identifiers
        	Map<Identifier, List<Identifier>> mdMap = packageMap.get(pid);
        	dp.setMetadataMap(mdMap);

        	// parse the metadata/data identifiers and store the associated objects if they are accessible
        	for (Identifier scienceMetadataId : mdMap.keySet()) {
        		dp.addAndDownloadData(scienceMetadataId);
        		List<Identifier> dataIdentifiers = mdMap.get(scienceMetadataId);
        		for (Identifier dataId : dataIdentifiers) {
        			dp.addAndDownloadData(dataId);
        		}
        	}
        }
        return dp;
    }
    
    /**
     * @return the metadata map
     */
    public Map<Identifier, List<Identifier>> getMetadataMap() {
        return metadataMap;
    }
    
    /**
     * Convenience function for working with the metadata map. Does a reverse
     * lookup to get the metadata object that is defined to document the provided
     * data object.  Returns null if the relationship has not been defined.
     * @param dataObject
     * @return
     */
    public Identifier getDocumentedBy(Identifier dataObject) {
    	Map<Identifier, List<Identifier>> mdMap = getMetadataMap();
    	Set<Identifier> metadataMembers = mdMap.keySet();
    	Identifier documenter = null;
    	for (Identifier md : metadataMembers) {
    		if (mdMap.get(md).contains(dataObject)) {
    			documenter = md;
    			break;
    		}
    	}
    	return documenter;
    }
    

    /**
     * @param metadataMap the metadataMap to set
     */
    public void setMetadataMap(Map<Identifier, List<Identifier>> metadataMap) {
        this.metadataMap = metadataMap;
    }
    
    /**
     * Set the SystemMetadata object to this data package.
     * @param systemMetadata - the SystemMetadata object will be set.
     */
    public void setSystemMetadata(SystemMetadata systemMetadata) {
      this.systemMetadata = systemMetadata;
    }
    
    /**
     * Get the SystemMetadata object associated with the data package.
     *@return the SystemMetadata object associated with the data package.
     */
    public SystemMetadata getSystemMetadata() {
      return this.systemMetadata;
    }

    /**
     * Create a ResourceMap from the component D1Object instances in this DataPackage.
     * 
     * TODO: create a RM when science metadata is null
     * TODO: handle error conditions when data list is null
     * @throws OREException 
     * @throws URISyntaxException 
     */
   
 
    // TODO: needs unit test. 
//    /**
//     * Validates the data map by checking that all objects are also found
//     * in the relationship map.  Otherwise, a relationship needs to be defined, or
//     * an object removed from the data map.  getUncharacterizedMember
//     * @return true if all D1Objects in the data map are found in the relationship map
//     * @since v1.1.1 
//     */
//    public boolean validateDataMap() {
//    	if (getUncharacterizedMembers().isEmpty()) {
//    		return true;
//    	}
//    	return false;
//    }
 
    // TODO: needs unit test    
    /**
     * Returns a list of package members that are not in the relationship map
     * (implying that these are not true members, as they will
     * not be included in the serialized ResourceMap).
     * @return Set<Identifier> : an identifier list
     * @since v1.1.1
     */
    public Set<Identifier> getUncharacterizedMembers() {
    	Set<Identifier> unmappedMembers = new HashSet<Identifier>();
    	Iterator<Identifier> it = objectStore.keySet().iterator();
    	while (it.hasNext()) {
    		Identifier pid = it.next();
    		if (!getMetadataMap().containsKey(pid) && (getDocumentedBy(pid) == null)) {
    			unmappedMembers.add(pid);
    		}
    	}
    	return unmappedMembers;
    }

    
    // TODO: needs unit test    
//    /**
//     * Validates the relationship map to ensure that all of the objects in the
//     * relationship map are either in the package locally (added to the data map),
//     * or are resolvable against the CN.
//     * @return boolean
//     * @throws InvalidToken
//     * @throws ServiceFailure
//     * @throws NotImplemented
//     * @since v1.1.1
//     */
//    public boolean validateRelationshipMap() 
//    throws InvalidToken, ServiceFailure, NotImplemented {
//    	if(getUnresolvableMembers().isEmpty()) {
//    		return true;
//    	}
//    	return false;
//    }
    
    /**
     * Returns the Identifiers in the relationship map that cannot
     * be found in the object map or resolved by the CN. (Indicating that
     * if the DataPackage is submitted as is, it will refer to non-existent
     * objects).
     * 
     * @return Set of unresolvable package resources
     * @throws InvalidToken
     * @throws ServiceFailure
     * @throws NotImplemented
     * @since v1.1.1
     */
    public Set<Identifier> getUnresolvableMembers() 
    throws InvalidToken, ServiceFailure, NotImplemented {
    	
    	Set<Identifier> unresolvedItems = getPackageResources();

    	unresolvedItems.removeAll(objectStore.keySet());
    	Iterator<Identifier> it = unresolvedItems.iterator();
    	while (it. hasNext()) {
    		Identifier item = it.next();
    		try {
    			D1Client.getCN().resolve(item);
    			unresolvedItems.remove(item);
    		} catch (NotAuthorized e) {
    			// counts as exists, so remove from the list
    			unresolvedItems.remove(item);
    		} catch (NotFound e) {
    			; // keep in the relationshoItems set
    		}
    	}
    	return unresolvedItems;
    }
    
    /**
     * Returns the set of Identifiers that are in the relationship map
     * (metadataMap)
     * @return
     */
    private Set<Identifier> getPackageResources() {
    	Set<Identifier> packageResources = new HashSet<Identifier>();
    	for (Identifier pid: getMetadataMap().keySet()) {
    		packageResources.add(pid);
    		packageResources.addAll(getMetadataMap().get(pid));
    	}
    	return packageResources;
    }
}