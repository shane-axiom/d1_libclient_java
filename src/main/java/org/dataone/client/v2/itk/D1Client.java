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

package org.dataone.client.v2.itk;

import java.net.URI;

import org.dataone.client.NodeLocator;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.rest.DefaultHttpMultipartRestClient;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.client.utils.ExceptionUtils;
import org.dataone.client.v2.impl.SettingsContextNodeLocator;
import org.dataone.client.v2.CNode;
import org.dataone.client.v2.MNode;
import org.dataone.client.v2.impl.D1NodeFactory;
import org.dataone.client.v2.impl.NodeListNodeLocator;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Session;

/**
 * The D1Client class represents a client-side implementation of the DataONE
 * Service API. The class exposes the DataONE APIs as client methods, dispatches
 * the calls to the correct DataONE node, and then returns the results or throws
 * the appropriate exceptions.  
 */
public class D1Client {

    private static NodeLocator nodeLocator;
    protected static MultipartRestClient restClient;
    
    
    /**
	 * Get the cached CNode object for calling Coordinating Node services.
	 * By default returns the production context CN, defined via the property 
	 * "D1Client.CN_URL". Use of D1Client in other contexts (non-production) 
	 * requires overriding or changing this property name, or calling the setCN
	 * method.
	 * See org.dataone.configuration.Settings class for details.
	 * 
	 * Connects using the default session / certificate 
     * @return the cn
	 * @throws ServiceFailure 
     * @throws NotImplemented 
     */
    public static CNode getCN() 
    throws ServiceFailure, NotImplemented 
    {
        return getCN((Session)null);
    }
    
    
	/**
	 * Get the cached CNode object for calling Coordinating Node services.
	 * By default returns the production context CN, defined via the property 
	 * "D1Client.CN_URL".  Use of D1Client in other contexts (non-production) 
	 * requires overriding or changing this property name, or calling the setCN
	 * method  
	 * See org.dataone.configuration.Settings class for details.
	 * 
	 * @param session - the client session to be used in connections, null uses default behavior. 
     * @return the cn
	 * @throws ServiceFailure 
	 * @throws NotImplemented 
     */
    public static CNode getCN(Session session) 
    throws ServiceFailure, NotImplemented {
        if (restClient == null) {
        	//TODO work session into restClient
        	restClient = new DefaultHttpMultipartRestClient();
        }
        try { 
        	if (nodeLocator == null) {
        		nodeLocator = new SettingsContextNodeLocator(restClient);	
        	}
        	return (CNode) nodeLocator.getCNode();
        } catch (ClientSideException e) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
		}
    }

    
    /**
     * Use this method to set the environment via the baseUrl to the environment's
     * Coordinating Node.  Doing so affects future calls using the NodeReferences -
     * only nodes registered in the context of the current CN will be findable.
     * 
     * @param cnUrl
     * @throws NotImplemented
     * @throws ServiceFailure
     */
    public static void setCN(String cnUrl) 
    throws NotImplemented, ServiceFailure 
    {         	
    	if (restClient == null) {
    		restClient = new DefaultHttpMultipartRestClient();
    	}
    	try {
    		CNode cn = D1NodeFactory.buildCNode(restClient, URI.create(cnUrl));
    		nodeLocator = new NodeListNodeLocator(cn.listNodes(), restClient);
    	} catch (ClientSideException e) {
			ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
		}
    }


    /**
     * Returns a Member Node using the base service URL for the node.
     * @param mnBaseUrl the service URL for the Member Node
     * @return the mn at a particular URL
     * @throws ServiceFailure 
     */
    public static MNode getMN(String mnBaseUrl) throws ServiceFailure {
    	MNode mn = null;
    	if (restClient == null) {
    		restClient = new DefaultHttpMultipartRestClient();
    	}
    	try {
    		if (nodeLocator != null) {
    			mn = (MNode) nodeLocator.getNode(mnBaseUrl);	
    		} 
    		mn = D1NodeFactory.buildMNode(restClient, URI.create(mnBaseUrl));
		} catch (ClientSideException e) {
			try {
				mn = D1NodeFactory.buildMNode(restClient, URI.create(mnBaseUrl));
				if (nodeLocator != null) {
					nodeLocator.putNode(mn.getNodeId(), mn);
				}
			}
			catch (ClientSideException cse) {
				throw ExceptionUtils.recastClientSideExceptionToServiceFailure(cse);
			}
		}
    	return mn;
    }


    /**
     * Returns a Coordinating Node using the base service URL to look up the node
     * in the existing environment.
     *  
     * @param cnBaseUrl
     * @return
     * @throws ServiceFailure 
     */
    //TODO: do we need this method?  When do we need to micro-manage which CN to connect to?
    public static CNode getCN(String cnBaseUrl) throws ServiceFailure {
    	CNode cn = null;
    	if (restClient == null) {
    		restClient = new DefaultHttpMultipartRestClient();
    	}
    	try {
    		if (nodeLocator != null) {
    			cn = (CNode) nodeLocator.getNode(cnBaseUrl);
    		}
    		cn = D1NodeFactory.buildCNode(restClient, URI.create(cnBaseUrl));
		} catch (ClientSideException e) {
			try {
				cn = D1NodeFactory.buildCNode(restClient, URI.create(cnBaseUrl));
				if (nodeLocator != null) {
					nodeLocator.putNode(cn.getNodeId(), cn);
				}
			}
			catch (ClientSideException cse) {
				throw ExceptionUtils.recastClientSideExceptionToServiceFailure(cse);
			}
		}
    	return cn;
    }


    /**
     * Return an MNode using the nodeReference
     * for the member node.  D1Client's cn instance will look up the
     * member node's baseURL from the passed in nodeReference
     * 
     * @param nodeRef
     * @return
     * @throws ServiceFailure
     */
    public static MNode getMN(NodeReference nodeRef) throws ServiceFailure 
    {  	
    	if (nodeLocator == null) {
    		try {
				nodeLocator = new NodeListNodeLocator(D1Client.getCN().listNodes(), 
						new DefaultHttpMultipartRestClient());
			} catch (NotImplemented e) {
				throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
			} catch (ClientSideException e) {
				ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
			}
    	}
    	MNode mn;
		try {
			mn = (MNode) nodeLocator.getNode(nodeRef);
		} catch (ClientSideException e) {
			throw new ServiceFailure("0000", "Node is not an MNode: "
   				 + nodeRef.getValue());
		}
    	if (mn == null) {
    		throw new ServiceFailure("0000", "Failed to find baseUrl for node "
    				 + nodeRef.getValue() + " in the NodeList");
    	}
        mn.setNodeId(nodeRef);
        return mn;
    }
    
    
    
    

    
    /**
     * Attempts to create the DataPackage.  First makes sure there is a D1Object
     * representing the ORE resource map, then delegates D1Object creation to 
     * D1Client.create(D1Object),
     * 
     * @since Not Implemented - need to determine correct assumptions and behavior
     * 
     * @param session
     * @param dataPackage
     * @return
     * @throws NotImplemented 
     */
    
    /* TODO: determine how to identify which objects are already created
     * TODO: determine behavior under situations where exceptions thrown half-way 
     * through.  Cannot package into a transaction.  
     * data objects and science metadata objects that don't already exist on a MN and then
     * create the ORE resource map on the MN
     */   
//    public static Identifier create(Session session, DataPackage dataPackage) throws NotImplemented {
//    	throw new NotImplemented("Client Exception", "this method has not been implemented yet.");
//    }

}
