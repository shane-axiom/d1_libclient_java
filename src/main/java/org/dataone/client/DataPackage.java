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
 * TODO: add ability to call MN.create() for the package, which should create() any
 * data objects and science metadata objects that don't already exist on a MN and then
 * create the ORE resource map on the MN
 */
public class DataPackage {
    
    private Identifier packageId;
    private D1Object scienceMetadata;
    private HashMap<Identifier, D1Object> dataObjects;
    private ResourceMap map = null;
    
    private boolean validPackage = false; 
    
    /**
     * Construct a data package, initializing member variables.
     */
    private DataPackage() {
        super();
        dataObjects = new HashMap<Identifier, D1Object>();
        setValidPackage(true);
    }
    
    /**
     * Construct a DataPackage using the given identifier to identify this package.  
     * The id is used as the identifier of the associated ORE map for this package.
     * @param id the Identifier of the package
     */
    public DataPackage(Identifier id) {
        this();
        setPackageId(id);
    }
    
    /**
     * Add a new object with the given Identifier to the package. This creates
     * a D1Object to wrap the identified object after downloading it from DataONE.
     * @param id the identifier of the object to be added
     */
    public void addData(Identifier id) {
        if (!contains(id)) {
            D1Object o = D1Object.download(id);
            if (o != null) {
                dataObjects.put(id, o);
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
                dataObjects.put(id, obj);
            }
        }
    }    
    
    /**
     * @return the scienceMetadata
     */
    public D1Object getScienceMetadata() {
        return scienceMetadata;
    }

    /**
     * Register the science metadata object with the given Identifier to the package. 
     * This creates a D1Object to wrap the identified science metadata object after 
     * downloading it from DataONE.
     * @param id the identifier of the object to be added
     */
    public void setScienceMetadata(Identifier id) {
            D1Object o = D1Object.download(id);
            if (o != null) {
                scienceMetadata = o;
            }
    }
    
    /**
     * Register an object as science metadata directly in the package without 
     * downloading it from a node. 
     * The identifier for this object is extracted from its system metadata.
     * @param obj the D1Object to be added
     */
    public void setScienceMetadata(D1Object obj) {
        if (obj != null) {
            scienceMetadata = obj;
        }
    }   
    
    /**
     * @return the number of objects in this package
     */
    public int size() {
        return dataObjects.size();
    }
    
    /**
     * Determine if an object with the given Identifier is already present in the package.
     * @param id the Identifier to be checked
     * @return boolean true if the Identifier is in the package
     */
    public boolean contains(Identifier id) {
        return dataObjects.containsKey(id);
    }
    
    /**
     * Get the D1Object associated with a given Identifier.
     * @param id the identifier of the object to be retrieved
     * @return the D1Object for that identifier, or null if not found
     */
    public D1Object get(Identifier id) {
        return dataObjects.get(id);
    }
    
    /**
     * Remove an object from a DataPackage based on its Identifier.
     * @param id the Identifier of the object to be removed.
     */
    public void remove(Identifier id) {
        dataObjects.remove(id);
    }
    
    /**
     * Return the set of Identifiers that are part of this package.
     * @return a Set of Identifiers in the package
     */
    public Set<Identifier> identifiers() {
        return dataObjects.keySet();
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
     * @param validPackage the validPackage to set
     */
    public void setValidPackage(boolean validPackage) {
        this.validPackage = validPackage;
    }

    /**
     * @return the validPackage
     */
    public boolean isValidPackage() {
        return validPackage;
    }

    /**
     * Return an ORE ResourceMap describing this package.
     * @return the map
     */
    public ResourceMap getMap() {
        if (null == map) {
            updateResourceMap();
        }
        return map;
    }
    
    /**
     * Return an ORE ResourceMap describing this package, serialized as an RDF graph.
     * @return the map as a serialized String
     */
    public String serializePackage() {
        ResourceMap rm = getMap();
        String rdfXml;
        try {
            rdfXml = ResourceMapFactory.getInstance().serializeResourceMap(rm);
        } catch (ORESerialiserException e) {
            setValidPackage(false);
            rdfXml = "";
        }
        return rdfXml;
    }
    
    // TODO: create a deserializePackage() method
    // INCOMPLETE METHOD IMPLEMENTATION!
    public static DataPackage deserializePackage(String resourceMap) {
        DataPackage dp = new DataPackage();
        try {
            Map<Identifier, List<Identifier>> rmData = ResourceMapFactory.getInstance().parseResourceMap(resourceMap);
            // TODO: where is the packageId represented in the return from the map processing?
            for (Identifier scienceMetadataId : rmData.keySet()) {
                dp.setScienceMetadata(scienceMetadataId);
                List<Identifier> dataIdentifiers = rmData.get(scienceMetadataId);
                for (Identifier dataId : dataIdentifiers) {
                    dp.addData(dataId);
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
     * Create a ResourceMap from the component D1Object instances in this DataPackage.
     * TODO: create a RM when science metadata is null
     * TODO: handle error conditions when data list is null
     */
    private void updateResourceMap() {
        
        List<Identifier> dataIdentifiers = new ArrayList<Identifier>(dataObjects.keySet());
        Map<Identifier, List<Identifier>> idMap = new HashMap<Identifier, List<Identifier>>();
        idMap.put(scienceMetadata.getIdentifier(), dataIdentifiers);
        try {
            map = ResourceMapFactory.getInstance().createResourceMap(packageId, idMap);
        } catch (OREException e) {
            // TODO: decide how to deal with packages cleanly that are missing information
            setValidPackage(false);
            map = null;
        } catch (URISyntaxException e) {
            // TODO: decide how to deal with packages cleanly that are missing information
            setValidPackage(false);
            map = null;
        }
    }
    
    /**
     * Bootstrap the package from a file, downloading all associated objects by 
     * recursing through the describes and describedBy lists.
     * @param id the identifier to be used in bootstrapping a package
     */
/*  
 * TODO: this method is obsolete and can probably be removed now 
    private void buildPackage(Identifier id) {
        
        // TODO: refactor to use ORE map rather than describes/describedBy
        // This current logic is obsolete
        if (!contains(id)) {

            // Add the object itself to the package
            addData(id);

            // Add all of the objects that this one describes
//            List<Identifier> describes = get(id).getDescribeList();
//            for (Identifier current_id : describes) {
//                buildPackage(current_id);
//            }
//
//            // Add all of the objects that this id is described by
//            List<Identifier> describedBy = get(id).getDescribeByList();
//            for (Identifier current_id : describes) {
//                buildPackage(current_id);
//            }
        }
    }
*/
}