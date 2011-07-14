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

import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.NodeReference;

/**
 * The D1Client class represents a client-side implementation of the DataONE
 * Service API. The class exposes the DataONE APIs as client methods, dispatches
 * the calls to the correct DataONE node, and then returns the results or throws
 * the appropriate exceptions.
 */
public class D1Client {

    private static CNode cn = null;
    
	/**
	 * Get the client instance of the Coordinating Node object for calling Coordinating Node services.
	 * By default returns the production context CN.  Instantiate a D1Client with different CN url to
	 * use different contexts.  
	 * 
     * @return the cn
	 * @throws ServiceFailure 
     */
    public static CNode getCN() throws ServiceFailure {
        if (cn == null) {
        	// get the CN URL
            String cnUrl = Settings.getConfiguration().getString("D1Client.CN_URL");

        	// determine which implementation to instantiate
        	String cnClassName = Settings.getConfiguration().getString("D1Client.cnClassName");
        	
        	if (cnClassName != null) {
        		// construct it using reflection
            	try {
					cn = (CNode) Class.forName(cnClassName).newInstance();
				} catch (Exception e) {
					throw D1Node.recastClientSideExceptionToServiceFailure(e);

				}
            	cn.setNodeBaseServiceUrl(cnUrl);
        	} else {
        		// default
                cn = new CNode(cnUrl);
        	}
        }
        return cn;
    }
    
    /**
     * Construct and return a Member Node using the base service URL for the node.
     * @param mnBaseUrl the service URL for the Member Node
     * @return the mn at a particular URL
     */
    public static MNode getMN(String mnBaseUrl) {
        MNode mn = new MNode( mnBaseUrl);
        return mn;
    }
    
    
    /**
     * Construct and return a Member Node using the nodeReference
     * for the member node.  D1Client's cn instance will look up the
     * member node's baseURL from the passed in nodeReference
     * 
     * @param nodeRef
     * @return
     * @throws ServiceFailure
     */
    public static MNode getMN(NodeReference nodeRef) throws ServiceFailure {
    	CNode cn;
		try {
			cn = getCN();
		} catch (Exception e) {
			throw D1Node.recastClientSideExceptionToServiceFailure(e);
		}
    	String mnBaseUrl = null;
		try {
			mnBaseUrl = cn.lookupNodeBaseUrl(nodeRef.getValue());
		} catch (NotImplemented e) {
			throw D1Node.recastClientSideExceptionToServiceFailure(e);
		}
        MNode mn = new MNode(mnBaseUrl);
        return mn;
    }
}
