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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

import org.dataone.client.types.ObsoletesChain;
import org.dataone.configuration.Settings;
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

    private static CNode cn = null;
    
	/**
	 * Get the client instance of the Coordinating Node object for calling Coordinating Node services.
	 * By default returns the production context CN, defined via the property "D1Client.CN_URL".
	 * Use of D1Client in other contexts (non-production) requires overriding or changing this 
	 * property name.  See org.dataone.configuration.Settings class for details.
	 * 
	 * @param session - the client session to be used in connections, null uses default behavior. 
     * @return the cn
	 * @throws ServiceFailure 
     */
    public static CNode getCN(Session session) throws ServiceFailure {
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
                cn = new CNode(cnUrl, session);
        	}
        }
        return cn;
    }
    
    
    /**
	 * Get the client instance of the Coordinating Node object for calling Coordinating Node services.
	 * By default returns the production context CN, defined via the property "D1Client.CN_URL".
	 * Use of D1Client in other contexts (non-production) requires overriding or changing this 
	 * property name.  See org.dataone.configuration.Settings class for details.
	 * 
	 * Connects using the default session / certificate 
     * @return the cn
	 * @throws ServiceFailure 
     */
    public static CNode getCN() throws ServiceFailure {
        return getCN(null);
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
			if (mnBaseUrl == null) 
				throw new ServiceFailure("0000","Failed to find baseUrl for node " + nodeRef.getValue() + " in the NodeList");
		} catch (NotImplemented e) {
			throw D1Node.recastClientSideExceptionToServiceFailure(e);
		}
        MNode mn = new MNode(mnBaseUrl);
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

    	String mn_url = D1Client.getCN().lookupNodeBaseUrl(sysmeta.getOriginMemberNode().getValue());
    	MNode mn = D1Client.getMN(mn_url);
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
     * @since v1.1.1
     */
    public static ObsoletesChain listUpdateHistory(Identifier pid) 
    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented 
    {
    	Identifier startingPid = pid;
    	ObsoletesChain chain = new ObsoletesChain(startingPid);

    	SystemMetadata smd = getCN().getSystemMetadata(startingPid, true);
    	chain.addObject(
    			pid, 
    			smd.getDateUploaded(), 
    			smd.getObsoletes(), 
    			smd.getObsoletedBy(), 
    			smd.getArchived());

		Identifier fpid = smd.getObsoletedBy();
		Identifier bpid = smd.getObsoletes();
		
    	while (fpid != null) {
        	smd = getCN().getSystemMetadata(fpid, true);
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
    		smd = getCN().getSystemMetadata(bpid, true);
    		chain.addObject(
        			bpid, 
        			smd.getDateUploaded(), 
        			smd.getObsoletes(), 
        			smd.getObsoletedBy(), 
        			smd.getArchived());
    		bpid = smd.getObsoletes();
    	}
    	return chain;
    }
}
