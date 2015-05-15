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

import java.io.IOException;
import java.net.URI;

import org.dataone.client.D1NodeFactory;
import org.dataone.client.NodeLocator;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.rest.HttpMultipartRestClient;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.client.utils.ExceptionUtils;
import org.dataone.client.v2.CNode;
import org.dataone.client.v2.MNode;
import org.dataone.client.v2.impl.NodeListNodeLocator;
import org.dataone.client.v2.impl.SettingsContextNodeLocator;
import org.dataone.service.cn.v2.CNCore;
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
    
	protected static MultipartRestClient multipartRestClient;
    
	protected static MultipartRestClient getMultipartRestClient() throws IOException, ClientSideException {
	    if (multipartRestClient == null) {
	        multipartRestClient = new HttpMultipartRestClient();
	    }
	    return multipartRestClient;
	}
	
	public static void setAuthToken(String authToken) {
		
		if (multipartRestClient != null && multipartRestClient instanceof HttpMultipartRestClient) {
			((HttpMultipartRestClient) multipartRestClient).setHeader("Authorization", "Bearer " + authToken);
		}

	}
		    
	/**
	 * For testing, we can override the nodeLocator
	 * @param nodeLocator
	 */
	public static void setNodeLocator(NodeLocator nodeLocator) {
		D1Client.nodeLocator = nodeLocator;
	}
	
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
        
    	CNode cn = null;
    	
        try { 
        	if (nodeLocator == null) {
        		nodeLocator = new SettingsContextNodeLocator(getMultipartRestClient());	
        	}
        	cn = (CNode) nodeLocator.getCNode();
        } catch (Exception e) {
        	try {
        		// create an empty NodeListNodeLocator to leverage the getCNode() 
				nodeLocator = new NodeListNodeLocator(null,getMultipartRestClient());
				cn = (CNode) nodeLocator.getCNode();
			} catch (ClientSideException | IOException e1) {
				throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
			}
        }
        return cn;
    }

    
    /**
     * Use this method to set the environment via the baseUrl to the environment's
     * Coordinating Node.  Doing so affects future calls using the NodeReferences -
     * only nodes registered in the context of the current CN will be findable
     * by NodeReference.
     * 
     * @param cnUrl
     * @throws NotImplemented
     * @throws ServiceFailure
     */
    public static void setCN(String cnUrl) 
    throws NotImplemented, ServiceFailure 
    {         	
    	try {
            CNCore cn = D1NodeFactory.buildNode(CNCore.class, getMultipartRestClient(), URI.create(cnUrl));
    		nodeLocator = new NodeListNodeLocator(cn.listNodes(), getMultipartRestClient());
    	} catch (ClientSideException | IOException e) {
			ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
		}
    }


    /**
     * Returns a Member Node using the base service URL for the node.
     * @param mnBaseUrl the service URL for the Member Node
     * @return the mn at a particular URL
     * @throws ServiceFailure 
     */
    public static MNode getMN(String mnBaseUrl) throws ServiceFailure 
    {
    	MNode mn = null;	
    	if (nodeLocator != null) {
    		try {		
    			mn = (MNode) nodeLocator.getNode(mnBaseUrl);	
    		} 
    		catch (ClientSideException e) {
    			;  // that's ok, will try a different way
    		}
    	}
    	if (mn == null) {
    		try {
                mn = D1NodeFactory.buildNode(MNode.class, getMultipartRestClient(), URI.create(mnBaseUrl));
//    			if (nodeLocator != null) {
//    				// be opportunist, but don't be the first to call the CN (and initialize potentially wrong state.		
//    				nodeLocator.putNode(mn.getNodeId(), mn);
//    			}
    		}
			catch (ClientSideException | IOException cse) {
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
    public static CNode getCN(String cnBaseUrl) throws ServiceFailure 
    {
    	CNode cn = null;	
    	if (nodeLocator != null) {
    		try {		
    			cn = (CNode) nodeLocator.getNode(cnBaseUrl);	
    		} 
    		catch (ClientSideException e) {
    			;  // that's ok, will try a different way
    		}
    	}
    	if (cn == null) {
    		try {
                cn = D1NodeFactory.buildNode(CNode.class, getMultipartRestClient(), URI.create(cnBaseUrl));
//    			if (nodeLocator != null && cn != null) {
//    				// be opportunist, but don't be the first to call the CN (and initialize potentially wrong state.		
//    				nodeLocator.putNode(cn.getNodeId(), cn);
//    			}
    		}
			catch (ClientSideException | IOException cse) {
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
		MNode mn = null;
    	try {
			// initialize the environment or lack-thereof
			D1Client.getCN(); 
			mn = (MNode) nodeLocator.getNode(nodeRef);
		} catch (ClientSideException e) {
			throw new ServiceFailure("0000", "Node is not an MNode: "
   				 + nodeRef.getValue());
		} catch (NotImplemented e) {
			throw new ServiceFailure("0000", "Got 'NotImplemented' from getCN(): " + e.getDescription());
		}
    	if (mn == null) {
    		throw new ServiceFailure("0000", "Failed to find baseUrl for node "
    				 + nodeRef.getValue() + " in the NodeList");
    	}
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
