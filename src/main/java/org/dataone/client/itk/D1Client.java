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

package org.dataone.client.itk;

import java.io.IOException;
import java.net.URI;

import org.dataone.client.CNode;
import org.dataone.client.MNode;
import org.dataone.client.NodeLocator;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.impl.D1NodeFactory;
import org.dataone.client.impl.NodeListNodeLocator;
import org.dataone.client.impl.SettingsContextNodeLocator;
import org.dataone.client.impl.rest.DefaultHttpMultipartRestClient;
import org.dataone.client.impl.rest.HttpCNode;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.client.types.ObsoletesChain;
import org.dataone.client.utils.ExceptionUtils;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidSystemMetadata;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;

/**
 * The D1Client class represents a client-side implementation of the DataONE
 * Service API. The class exposes the DataONE APIs as client methods, dispatches
 * the calls to the correct DataONE node, and then returns the results or throws
 * the appropriate exceptions.  
 */
public class D1Client {

    private static NodeLocator nodeLocator;
    protected static MultipartRestClient restClient;
    private static final int DEFAULT_TIMEOUT_MILLIS = 30000; 
    
    
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
        	restClient = new DefaultHttpMultipartRestClient(DEFAULT_TIMEOUT_MILLIS);
        }
        try { 
        	if (nodeLocator == null) {
        		nodeLocator = new SettingsContextNodeLocator(restClient);	
        	}
        	return nodeLocator.getCNode();
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
    		restClient = new DefaultHttpMultipartRestClient(DEFAULT_TIMEOUT_MILLIS);
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
    		restClient = new DefaultHttpMultipartRestClient(DEFAULT_TIMEOUT_MILLIS);
    	}
    	try {
    		if (nodeLocator != null) {
    			mn = nodeLocator.getMNode(mnBaseUrl);	
    		} 
    		mn = D1NodeFactory.buildMNode(restClient, URI.create(mnBaseUrl));
		} catch (ClientSideException e) {
			try {
				mn = D1NodeFactory.buildMNode(restClient, URI.create(mnBaseUrl));
				if (nodeLocator != null) {
					nodeLocator.putMNode(mn.getNodeId(), mn);
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
    		restClient = new DefaultHttpMultipartRestClient(DEFAULT_TIMEOUT_MILLIS);
    	}
    	try {
    		if (nodeLocator != null) {
    			cn = nodeLocator.getCNode(cnBaseUrl);
    		}
    		cn = D1NodeFactory.buildCNode(restClient, URI.create(cnBaseUrl));
		} catch (ClientSideException e) {
			try {
				cn = D1NodeFactory.buildCNode(restClient, URI.create(cnBaseUrl));
				if (nodeLocator != null) {
					nodeLocator.putCNode(cn.getNodeId(), cn);
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
						new DefaultHttpMultipartRestClient(30));
			} catch (NotImplemented e) {
				throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
			} catch (ClientSideException e) {
				ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
			}
    	}
    	MNode mn;
		try {
			mn = nodeLocator.getMNode(nodeRef);
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
     * Attempts to create the given D1Object on the originMemberNode contained 
     * in its SystemMetadata.  Does not perform any identifier reservation checks
     * or make any reservations.
     * 
     * @param session
     * @param d1object - the d1object representing both the data bytes and systemMetadata
     * @return the Identifier returned from the mn.create call
     * 
     * @throws InvalidToken
     * @throws ServiceFailure
     * @throws NotAuthorized
     * @throws IdentifierNotUnique
     * @throws UnsupportedType
     * @throws InsufficientResources
     * @throws InvalidSystemMetadata
     * @throws NotImplemented
     * @throws InvalidRequest
     */
    public static Identifier create(Session session, D1Object d1object) throws InvalidToken, ServiceFailure, NotAuthorized, 
    IdentifierNotUnique, UnsupportedType, InsufficientResources, InvalidSystemMetadata, NotImplemented, InvalidRequest 
    {
    	SystemMetadata sysmeta = d1object.getSystemMetadata();
    	if (sysmeta == null) 
    		throw new InvalidRequest("Client Error", "systemMetadata of the D1Object cannot be null");

    	MNode mn = D1Client.getMN(sysmeta.getOriginMemberNode());
    	Identifier rGuid;
		try {
			rGuid = mn.create(session, sysmeta.getIdentifier(), 
					d1object.getDataSource().getInputStream(), sysmeta);
		} catch (IOException e) {
			throw new ServiceFailure("000 Client Exception","Could not open InputStream from the data: " + e.getMessage());
		}
    	return rGuid;
    }
    
    
    /**
     * Perform an update an object in DataONE with the given D1Object on the 
     * originMemberNode contained in its systemMetadata. 
     * 
     * For this operation to work, the D1Object's systemMetadata needs have the
     * obsoletes field set with the object to be updated, and the originMemberNode
     * needs to match the authoritativeMemberNode of the object being updated.
     * 
     * As with D1Client.create(), this method does not perform any identifier
     * reservation checks or make any reservations.
     * 
     * @param session
     * @param d1object - the d1object representing both the data bytes and systemMetadata
     * @return the Identifier returned from the mn.create call
     * 
     * @throws InvalidToken
     * @throws ServiceFailure
     * @throws NotAuthorized
     * @throws IdentifierNotUnique
     * @throws UnsupportedType
     * @throws InsufficientResources
     * @throws InvalidSystemMetadata
     * @throws NotImplemented
     * @throws InvalidRequest
     * @throws NotFound 
     * @since v1.2
     */
    public static Identifier update(Session session, D1Object d1object) 
    throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, 
    UnsupportedType, InsufficientResources, InvalidSystemMetadata, NotImplemented, 
    InvalidRequest, NotFound 
    {
    	SystemMetadata sysmeta = d1object.getSystemMetadata();
    	if (sysmeta == null) 
    		throw new InvalidRequest("Client Error", "systemMetadata of the D1Object cannot be null");

    	MNode mn = D1Client.getMN(sysmeta.getOriginMemberNode());
    	Identifier rGuid;
		try {
			rGuid = mn.update(sysmeta.getObsoletes(), d1object.getDataSource().getInputStream(), 
					sysmeta.getIdentifier(),sysmeta);
		} catch (IOException e) {
			throw new ServiceFailure("000 Client Exception","Could not open InputStream from the data: " + e.getMessage());
		}
    	return rGuid;
    }
    
    
    
    /**
     * Perform an archive on an object in DataONE with the given D1Object on the 
     * authoritativeMemberNode contained in its systemMetadata. 
     * 
     * @param session
     * @param d1object - the d1object representing both the data bytes and systemMetadata
     * @return the Identifier returned from the mn.archive call
     * 
     * @throws InvalidToken
     * @throws ServiceFailure
     * @throws NotAuthorized
     * @throws NotImplemented
     * @throws InvalidRequest
     * @throws NotFound
     * @throws ClientSideException 
     * @since v1.2 
     */
    public static Identifier archive(Session session, D1Object d1object) 
    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented, InvalidRequest 
    {
    	SystemMetadata sysmeta = d1object.getSystemMetadata();
    	if (sysmeta == null) 
    		throw new InvalidRequest("Client Error", "systemMetadata of the D1Object cannot be null");

    	MNode mn = D1Client.getMN(sysmeta.getAuthoritativeMemberNode());
    	Identifier rGuid;
		rGuid = mn.archive(d1object.getIdentifier());
    	return rGuid;
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
    
    
    /**
     * Return the full ObsoletesChain for the given Identifier.  Includes 
     * predecessors and antecedents. 
     * @param pid
     * @return a complete ObsoletesChain
     * @throws InvalidToken
     * @throws ServiceFailure
     * @throws NotAuthorized
     * @throws NotFound
     * @throws NotImplemented
     * @throws ClientSideException 
     * @since v1.2
     */
    public static ObsoletesChain listUpdateHistory(Identifier pid) 
    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented 
    {
    	Identifier startingPid = pid;
    	ObsoletesChain chain = new ObsoletesChain(startingPid);

    	SystemMetadata smd = null;
    	try {
    		smd = getSysmeta(startingPid);
    		chain.addObject(
    			pid, 
    			smd.getDateUploaded(), 
    			smd.getObsoletes(), 
    			smd.getObsoletedBy(), 
    			smd.getArchived());

    		Identifier fpid = smd.getObsoletedBy();
    		Identifier bpid = smd.getObsoletes();
		
    		while (fpid != null) {
    			smd = getSysmeta(fpid);
    			chain.addObject(
        			fpid, 
        			smd.getDateUploaded(), 
        			smd.getObsoletes(), 
        			smd.getObsoletedBy(), 
        			smd.getArchived());
    			fpid = smd.getObsoletedBy();
    		}
    	
    		// get the first obsoletes by looking up in the stored list
    		while (bpid != null) {
    			smd = getSysmeta(bpid);
    			chain.addObject(
        			bpid, 
        			smd.getDateUploaded(), 
        			smd.getObsoletes(), 
        			smd.getObsoletedBy(), 
        			smd.getArchived());
    			bpid = smd.getObsoletes();
    		}
    	} catch (NullPointerException npe) {
    		ServiceFailure sf = new ServiceFailure("0000", 
    				"Likely Null value for required systemMetadata field for: " + 
    				smd.getIdentifier() + npe.getMessage());
    		sf.setStackTrace(npe.getStackTrace());
    		throw sf;
    	}
    	return chain;
    }
    
    private static SystemMetadata getSysmeta(Identifier pid) 
    throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, NotImplemented 
    {
    	if (getCN() instanceof HttpCNode) {
    		return ((HttpCNode)getCN()).getSystemMetadata(pid, true);
    	
    	} else {
    		return getCN().getSystemMetadata(pid);
    	}
    }
}
