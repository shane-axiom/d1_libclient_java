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

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dataone.ore.ResourceMapFactory;
import org.dataone.service.types.v1.Identifier;
import org.dspace.foresite.OREException;
import org.dspace.foresite.OREParserException;
import org.dspace.foresite.ORESerialiserException;
import org.dspace.foresite.ResourceMap;

/**
 * A collection of @see D1Object that are interrelated as a package.  The 
 * DataPackage allows all of the science metadata, data objects, and system metadata
 * associated with those objects to be accessed in a common place.  Each DataPackage
 * contains one science metadata D1Object, and 0 or more data objects represented as 
 * D1Object instances.  The DataPackage can be serialized as an OAI-ORE ResourceMap
 * that details the linkages among data objects and science metadata objects.
 * 
 */
public class DataPackage {
    
    private Identifier packageId;
    private Map<Identifier, List<Identifier>> metadataMap;
    private HashMap<Identifier, D1Object> objectStore;
    private ResourceMap map = null;
    
        
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
        setPackageId(id);
    }
    
    /**
     * Add a new object with the given Identifier to the package. This creates
     * a D1Object to wrap the identified object after downloading it from DataONE.
     * @param id the identifier of the object to be added
     */
    public void addAndDownloadData(Identifier id) {
        if (!contains(id)) {
        	D1Object o = D1Object.download(id);
//            D1Object o = null;
//			try {
//				o = D1Object.download(id);
//			} catch (InvalidToken e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (NotAuthorized e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (NotFound e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (ServiceFailure e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (NotImplemented e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
            if (o != null) {
                objectStore.put(id, o);
            }
        }
    }
    
    /**
     * Add a new object directly to the package without downloading it from a node. 
     * The identifier for this object is extracted from its system metadata.
     * @param obj the D1Object to be added
     */
    public void addData(D1Object obj) {
        Identifier id = obj.getIdentifier();
        if (!contains(id)) {
            if (obj != null) {
                objectStore.put(id, obj);
            }
        }
    }
        
    public void insertRelationship(Identifier metadataID, List<Identifier> dataIDList) {
        List<Identifier> associatedData = null;
        
        // Determine if the metadata object is already in the relations list
        // Use it if so, if not then create a list for this metadata link
        if (metadataMap == null)
        	metadataMap = new HashMap<Identifier, List<Identifier>>();
        
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
     * @return the number of objects in this package
     */
    public int size() {
        return objectStore.size();
    }
    
    /**
     * Determine if an object with the given Identifier is already present in the package.
     * @param id the Identifier to be checked
     * @return boolean true if the Identifier is in the package
     */
    public boolean contains(Identifier id) {
        return objectStore.containsKey(id);
    }
    
    /**
     * Get the D1Object associated with a given Identifier.
     * @param id the identifier of the object to be retrieved
     * @return the D1Object for that identifier, or null if not found
     */
    public D1Object get(Identifier id) {
        return objectStore.get(id);
    }
    
    /**
     * Remove an object from a DataPackage based on its Identifier.
     * @param id the Identifier of the object to be removed.
     */
    public void remove(Identifier id) {
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
     * Return an ORE ResourceMap describing this package.
     * @return the map
     * @throws URISyntaxException 
     * @throws OREException 
     */
    public ResourceMap getMap() throws OREException, URISyntaxException 
    {
        updateResourceMap();
        return map;
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
     * Deserialize an ORE resourceMap by parsing it, extracting the associated package identifier,
     * and the list of metadata and data objects aggregated in the ORE Map.  Create an instance
     * of a DataPackage, and for each metadata and data object in the aggregation, add it to the
     * package.
     * @param resourceMap the string representation of an ORE map in XML format
     * @return DataPackage constructed from the map
     */
    public static DataPackage deserializePackage(String resourceMap) {
        DataPackage dp = null;
        try {
            Map<Identifier, Map<Identifier, List<Identifier>>> packageMap = 
            		ResourceMapFactory.getInstance().parseResourceMap(resourceMap);

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
            
            
        } catch (UnsupportedEncodingException e) {
            // TODO: these should probably be thrown as exceptions to avoid NPEs, but its not clear which would be appropriate
            dp = null;
        } catch (OREException e) {
            dp = null;
        } catch (URISyntaxException e) {
            dp = null;
        } catch (OREParserException e) {
            dp = null;
        } 
        return dp;
    }
    
    /**
     * @return the metadataMap
     */
    public Map<Identifier, List<Identifier>> getMetadataMap() {
        return metadataMap;
    }

    /**
     * @param metadataMap the metadataMap to set
     */
    public void setMetadataMap(Map<Identifier, List<Identifier>> metadataMap) {
        this.metadataMap = metadataMap;
    }

    /**
     * Create a ResourceMap from the component D1Object instances in this DataPackage.
     * 
     * TODO: create a RM when science metadata is null
     * TODO: handle error conditions when data list is null
     * @throws OREException 
     * @throws URISyntaxException 
     */
    private void updateResourceMap() throws OREException, URISyntaxException {
        
        //List<Identifier> dataIdentifiers = new ArrayList<Identifier>(objectStore.keySet());
        //Map<Identifier, List<Identifier>> idMap = new HashMap<Identifier, List<Identifier>>();
        //idMap.put(scienceMetadata.getIdentifier(), dataIdentifiers);
        try {
            map = ResourceMapFactory.getInstance().createResourceMap(packageId, metadataMap);
        } catch (OREException e) {        
            map = null;
            throw e;
        } catch (URISyntaxException e) {
            map = null;
            throw e;
        }
    }    
}