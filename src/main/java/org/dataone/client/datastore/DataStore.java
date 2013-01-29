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
package org.dataone.client.datastore;

import java.io.InputStream;

import org.dataone.service.types.v1.Identifier;

/**
 * An interface for storing and accessing the data objects
 * @author tao
 *
 */
public interface DataStore {
    
    /**
     * Get a data object in the InputStream format for a specified identifier.
     * @param identifier  the identifier of the data object
     * @return the data object in the OutputStream format
     * @throws Exception
     */
    public InputStream get(Identifier identifier) throws Exception;
    
    /**
     * Store the data object (in the InputSTream format) into the data store with the specified id.
     * @param identifier  the identifier of the data object
     * @param data  the data object as the InputStream object
     * @throws Exception
     */
    public void set(Identifier identifier, InputStream data) throws Exception;
    
}
