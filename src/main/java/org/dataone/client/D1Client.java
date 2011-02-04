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

/**
 * The D1Client class represents a client-side implementation of the DataONE
 * Service API. The class exposes the DataONE APIs as client methods, dispatches
 * the calls to the correct DataONE node, and then returns the results or throws
 * the appropriate exceptions.
 */
public class D1Client {

    private static String CN_URI = Settings.get("D1Client.CN_URL");

    private static CNode cn = null;

    /**
     * Construct a client instance, setting the URI to be used for the coordinating node
     * for this client.  This overrides the default, and typically need not be used
     * except for testing.  Typical usage is to call the static getCN() method to
     * find use the standard CN location.
     * @param cnUri the alternate CN to use for this client
     */
    public D1Client(String cnUri) {
        CN_URI = cnUri;
        cn = new CNode(CN_URI);
    }
    
	/**
	 * Get the client instance of the Coordinating Node object for calling Coordinating Node services.
	 * 
     * @return the cn
     */
    public static CNode getCN() {
        if (cn == null) {
            cn = new CNode(CN_URI);
        }
        return cn;
    }
    
    /**
     * Construct and return a Member Node using the base service URL for the node.
     * @param mnBaseUrl the service URL for the Member Node
     * @return the mn at a particular URL
     */
    public static MNode getMN(String mnBaseUrl) {
        MNode mn = new MNode(mnBaseUrl);
        return mn;
    }
}
