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

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.dataone.service.types.Identifier;

/**
 * A collection of @see D1Object that are interrelated as a package.  The 
 * DataPackage allows all of the science metadata, data objects, and system metadata
 * associated with those objects to be accessed in a common place.
 */
public class DataPackage {
    private HashMap<Identifier, D1Object> objects;

    // TODO: Once ORE is used for the maps, the package itself will have an Identifier, which needs to be tracked
    //private Identifier identifier;
    //private OREMap map;
    
    /**
     * Construct a data package, initializing member variables.
     */
    private DataPackage() {
        super();
        objects = new HashMap<Identifier, D1Object>();
    }
    
    /**
     * Construct a DataPackage using the given identifier to bootstrap the
     * contents of the package.  The id is used to locate an object in the
     * DataONE system, and all the other objects which it Describes or which
     * are describedBy it are also loaded into the package.
     * @param id the Identifier of the member of the package
     */
    public DataPackage(Identifier id) {
        this();
        buildPackage(id);
    }
    
    /**
     * Add a new object with the given Identifier to the package. This creates
     * a D1Object to wrap the identified object.
     * @param id the identifier of the object to be added
     */
    public void add(Identifier id) {
        if (!contains(id)) {
            D1Object o = new D1Object(id);
            if (o != null) {
                objects.put(id, o);
            }
        }
    }
    
    /**
     * @return the number of objects in this package
     */
    public int size() {
        return objects.size();
    }
    
    /**
     * Determine if an object with the given Identifier is already present in the package.
     * @param id the Identifier to be checked
     * @return boolean true if the Identifier is in the package
     */
    public boolean contains(Identifier id) {
        return objects.containsKey(id);
    }
    
    /**
     * Get the D1Object associated with a given Identifier.
     * @param id the identifier of the object to be retrieved
     * @return the D1Object for that identifier, or null if not found
     */
    public D1Object get(Identifier id) {
        return objects.get(id);
    }
    
    /**
     * Remove an object from a DataPackage based on its Identifier.
     * @param id the Identifier of the object to be removed.
     */
    public void remove(Identifier id) {
        objects.remove(id);
    }
    
    /**
     * Return the set of Identifiers that are part of this package.
     * @return a Set of Identifiers in the package
     */
    public Set<Identifier> identifiers() {
        return objects.keySet();
    }
    
    /**
     * Bootstrap the package from a file, downloading all associated objects by 
     * recursing through the describes and describedBy lists.
     * @param id the identifier to be used in bootstrapping a package
     */
    private void buildPackage(Identifier id) {
        
        if (!contains(id)) {

            // Add the object itself to the package
            add(id);

            // Add all of the objects that this one describes
            List<Identifier> describes = get(id).getDescribeList();
            for (Identifier current_id : describes) {
                buildPackage(current_id);
            }

            // Add all of the objects that this id is described by
            List<Identifier> describedBy = get(id).getDescribeByList();
            for (Identifier current_id : describes) {
                buildPackage(current_id);
            }
        }
    }
    
}