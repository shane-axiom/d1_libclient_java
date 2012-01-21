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

package org.dataone.client.exception;


/**
 * NotCached is thrown when an object is retrieved from a cache and found to 
 * not have been placed in the cache.
 * @author Matthew Jones
 */
public class NotCached extends Exception {

//    static Logger logger = Logger.getLogger(NotCached.class.getName());

    /**
     * Construct a NotCached exception with the message.
     * 
     * @param message the description of this exception
     */
    public NotCached(String message) {
        super(message);
    }
}
