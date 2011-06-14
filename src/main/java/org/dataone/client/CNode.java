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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.http.HttpException;
import org.apache.http.client.ClientProtocolException;
import org.dataone.cn.batch.utils.TypeMarshaller;
import org.dataone.mimemultipart.SimpleMultipartEntity;
import org.dataone.service.Constants;
import org.dataone.service.D1Url;
import org.dataone.service.EncodingUtilities;
import org.dataone.service.NodeListParser;
import org.dataone.service.cn.CNAuthorization;
import org.dataone.service.cn.CNCore;
import org.dataone.service.cn.CNIdentity;
import org.dataone.service.cn.CNRead;
import org.dataone.service.cn.CNRegister;
import org.dataone.service.cn.CNReplication;
import org.dataone.service.exceptions.AuthenticationTimeout;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidCredentials;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidSystemMetadata;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.UnsupportedMetadataType;
import org.dataone.service.exceptions.UnsupportedQueryType;
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.types.AccessPolicy;
import org.dataone.service.types.Checksum;
import org.dataone.service.types.Event;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.Log;
import org.dataone.service.types.Node;
import org.dataone.service.types.NodeList;
import org.dataone.service.types.NodeReference;
import org.dataone.service.types.ObjectFormat;
import org.dataone.service.types.ObjectFormatIdentifier;
import org.dataone.service.types.ObjectFormatList;
import org.dataone.service.types.ObjectList;
import org.dataone.service.types.ObjectLocationList;
import org.dataone.service.types.Person;
import org.dataone.service.types.QueryType;
import org.dataone.service.types.ReplicationPolicy;
import org.dataone.service.types.ReplicationStatus;
import org.dataone.service.types.Session;
import org.dataone.service.types.Subject;
import org.dataone.service.types.SubjectList;
import org.dataone.service.types.SystemMetadata;
import org.jibx.runtime.JiBXException;
import org.xml.sax.SAXException;

/**
 * CNode represents a DataONE Coordinating Node, and allows calling classes to
 * execute CN services.
 */
public class CNode extends D1Node 
implements CNCore, CNRead, CNAuthorization, CNIdentity, CNRegister, CNReplication 
{

    private Map<String, String> nodeMap;

    /**
     * Construct a Coordinating Node, passing in the base url for node services. The CN
     * first retrieves a list of other nodes that can be used to look up node
     * identifiers and base urls for further service invocations.
     *
     * @param nodeBaseServiceUrl base url for constructing service endpoints.
     */
    public CNode(String nodeBaseServiceUrl) {
        super(nodeBaseServiceUrl);
        try {
            initializeNodeMap();
        } catch (XPathExpressionException e) {
            nodeMap = new HashMap<String, String>();
            e.printStackTrace();
        } catch (IOException e) {
            nodeMap = new HashMap<String, String>();
            e.printStackTrace();
        } catch (SAXException e) {
            nodeMap = new HashMap<String, String>();
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            nodeMap = new HashMap<String, String>();
            e.printStackTrace();
        }
    }
    
    public ObjectList search(Session session, QueryType queryType, String query)
    throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
    NotImplemented
	{

        if (session == null) {
            session = new Session();
            session.setSubject(new Subject());
            session.getSubject().setValue("public");
        }
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);

        // set default params, if need be
        String paramAdditions = "";
        if (!query.contains("qt=solr")) {
            paramAdditions = "qt=solr&";
        }
        if (!query.contains("pageSize=\\d+")) {
            paramAdditions += "pageSize=200&";
        }
        if (!query.contains("start=\\d+")) {
            paramAdditions += "start=0&";
        }
        if (!query.contains("sessionid=")) {
            paramAdditions += "sessionid=" + session.getSubject().getValue() + "&";
        }
        String paramsComplete = paramAdditions + query;
        // clean up paramsComplete string
        if (paramsComplete.endsWith("&")) {
            paramsComplete = paramsComplete.substring(0, paramsComplete.length() - 1);
        }

        url.addPreEncodedNonEmptyQueryParams(paramsComplete);

        D1RestClient client = new D1RestClient(true, verbose);
        client.setHeader("session", session.getSubject().getValue());

        InputStream is = null;
        try {
            is = client.doGetRequest(url.getUrl());
        } catch (NotFound e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
        } catch (IdentifierNotUnique e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
        } catch (UnsupportedType e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
        } catch (InvalidSystemMetadata e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
        } catch (InvalidCredentials e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
        } catch (AuthenticationTimeout e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
        } catch (UnsupportedMetadataType e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
        } catch (ClientProtocolException e) {
            throw recastClientSideExceptionToServiceFailure(e);
        } catch (IllegalStateException e) {
            throw recastClientSideExceptionToServiceFailure(e);
        } catch (IOException e) {
            throw recastClientSideExceptionToServiceFailure(e);
        } catch (HttpException e) {
            throw recastClientSideExceptionToServiceFailure(e);
        }
        return (ObjectList) deserializeServiceType(ObjectList.class,is);

    }

    @Override
    public InputStream get(Session session, Identifier pid)
            throws NotAuthorized, NotImplemented, NotFound, ServiceFailure, InvalidToken, InvalidRequest {
        return super.get(session, pid);
    }

    @Override
    public SystemMetadata getSystemMetadata(Session session, Identifier pid)
        throws InvalidToken, NotImplemented, ServiceFailure, NotAuthorized, NotFound, InvalidRequest {
        return super.getSystemMetadata(session, pid);
    }

    
    @Override
    public ObjectLocationList resolve(Session session, Identifier pid)
    throws InvalidRequest, InvalidToken, ServiceFailure, NotAuthorized,
	NotFound, NotImplemented 
    {

        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_RESOLVE);
        url.addNextPathElement(pid.getValue());

        if (session == null) {
            session = new Session();
            session.setSubject(new Subject());
            session.getSubject().setValue("public");
        }
        url.addNonEmptyParamPair("sessionid", session.getSubject().getValue());

        D1RestClient client = new D1RestClient(true, verbose);
        client.setHeader("session", session.getSubject().getValue());

        InputStream is = null;
        try {
            is = client.doGetRequest(url.getUrl());
        } catch (IdentifierNotUnique e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
        } catch (UnsupportedType e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
        } catch (InvalidSystemMetadata e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
        } catch (InvalidCredentials e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
        } catch (AuthenticationTimeout e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
        } catch (UnsupportedMetadataType e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
        } catch (ClientProtocolException e) {
            throw recastClientSideExceptionToServiceFailure(e);
        } catch (IllegalStateException e) {
            throw recastClientSideExceptionToServiceFailure(e);
        } catch (IOException e) {
            throw recastClientSideExceptionToServiceFailure(e);
        } catch (HttpException e) {
            throw recastClientSideExceptionToServiceFailure(e);
        }
        return (ObjectLocationList) deserializeServiceType(ObjectLocationList.class,is);
    }
    

    /**
     * create both a system metadata resource and science metadata resource with
     * the specified pid
     */
    @Override
    public Identifier create(Session session, Identifier pid,
    InputStream object, SystemMetadata sysmeta) 
    throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique,
    UnsupportedType, InsufficientResources, InvalidSystemMetadata, NotImplemented,
    InvalidRequest
    {
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
        url.addNextPathElement(pid.getValue());

        if (session != null) {
            url.addNonEmptyParamPair("sessionid", session.getSubject().getValue());
        }


        SimpleMultipartEntity mpe = new SimpleMultipartEntity();

        // Coordinating Nodes must maintain systemmetadata of all object on dataone
        // however Coordinating nodes do not house Science Data only Science Metadata
        // Thus, the inputstream for an object may be null
        // so deal with it here ...
        // and this is how CNs are different from MNs
        // because if object is null on an MN, we should throw an exception

        try {
        	if (object == null) {
        		// object sent is an empty string
        		mpe.addFilePart("object", "");
        	} else {
        		mpe.addFilePart("object", object);
        	}
        	mpe.addFilePart("sysmeta", sysmeta, SystemMetadata.class);
        } catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (JiBXException e) {
			throw recastClientSideExceptionToServiceFailure(e);	
		}

        D1RestClient client = new D1RestClient(true, verbose);
        InputStream is = null;

        try {
            is = client.doPostRequest(url.getUrl(), mpe);
        } catch (NotFound e) {
            throw new ServiceFailure("1090", e.getClass().getSimpleName() + ": " + e.getDetail_code() + ": " + e.getDescription());
        } catch (InvalidCredentials e) {
            throw new ServiceFailure("1090", e.getClass().getSimpleName() + ": " + e.getDetail_code() + ": " + e.getDescription());
        } catch (AuthenticationTimeout e) {
            throw new ServiceFailure("1090", e.getClass().getSimpleName() + ": " + e.getDetail_code() + ": " + e.getDescription());
        } catch (UnsupportedMetadataType e) {
            throw new ServiceFailure("1090", e.getClass().getSimpleName() + ": " + e.getDetail_code() + ": " + e.getDescription());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (ClientProtocolException e) {
            throw new ServiceFailure("1090", e.getClass().getSimpleName() + ": " + e.getMessage());
        } catch (IllegalStateException e) {
            throw new ServiceFailure("1090", e.getClass().getSimpleName() + ": " + e.getMessage());
        } catch (IOException e) {
            throw new ServiceFailure("1090", e.getClass().getSimpleName() + ": " + e.getMessage());
        } catch (HttpException e) {
            throw new ServiceFailure("1090", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        return (Identifier) deserializeServiceType(Identifier.class, is);
    }

    /**
     * update a resource with the specified pid.
     */
//    public Identifier update(Session session, Identifier pid,
//            InputStream object, Identifier newPid, SystemMetadata sysmeta)
//            throws InvalidToken, ServiceFailure, NotAuthorized,
//            IdentifierNotUnique, UnsupportedType, InsufficientResources,
//            NotFound, InvalidSystemMetadata, NotImplemented {
//
//
//        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
//        url.addNextPathElement(pid.getValue());
//        if (session != null) {
//            url.addNonEmptyParamPair("sessionid", session.getSubject().getValue());
//        }
//
//        SimpleMultipartEntity mpe = new SimpleMultipartEntity();
//        mpe.addParamPart("newPid",
//                EncodingUtilities.encodeUrlQuerySegment(newPid.getValue()));
//        // Coordinating Nodes must maintain systemmetadata of all object on dataone
//        // however Coordinating nodes do not house Science Data only Science Metadata
//        // Thus, the inputstream for an object may be null
//        // so deal with it here ...
//        // and this is how CNs are different from MNs
//        // because if object is null on an MN, we should throw an exception
//
//        try { 
//        	if (object == null) {
//        		mpe.addFilePart("object", "");
//        	} else {
//        		mpe.addFilePart("object", object);
//        	}
//        } catch (IOException e) {
//        	throw new ServiceFailure("1090", 
//        			"IO Exception creating the filepart for object: "
//        			+ e.getMessage());	
//        }
//        try {
//            mpe.addFilePart("sysmeta", sysmeta, SystemMetadata.class); 
//        } catch (JiBXException e) {
//            throw new ServiceFailure("1090",
//                    "Could not serialize the systemMetadata: "
//                    + e.getMessage());
//        } catch (IOException e) {
//        	throw new ServiceFailure("1090", 
//        			"IO Exception creating the filepart for systemMetadata: "
//        			+ e.getMessage());	
//        }
//
//
//        D1RestClient client = new D1RestClient();
//        InputStream is = null;
//
//        try {
//            is = client.doPutRequest(url.getUrl(), mpe);
//        } catch (NotFound e) {
//            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
//        } catch (InvalidCredentials e) {
//            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
//        } catch (InvalidRequest e) {
//            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
//        } catch (AuthenticationTimeout e) {
//            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
//        } catch (UnsupportedMetadataType e) {
//            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
//		} catch (UnsupportedQueryType e) {
//			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
//		} catch (ClientProtocolException e) {
//            throw recastClientSideExceptionToServiceFailure(e);
//        } catch (IllegalStateException e) {
//            throw recastClientSideExceptionToServiceFailure(e);
//        } catch (IOException e) {
//            throw recastClientSideExceptionToServiceFailure(e);
//        } catch (HttpException e) {
//            throw recastClientSideExceptionToServiceFailure(e);
//        }
//        return (Identifier) deserializeServiceType(Identifier.class, is);
//    }

    @Override
    public Identifier reserveIdentifier(Session session, Identifier pid, 
        String scope, String format) throws InvalidToken, ServiceFailure, 
        NotAuthorized, IdentifierNotUnique, InvalidRequest, NotImplemented {
            throw new NotImplemented("4191", "Client does not implement this method.");
    }

    /* TODO: Remove
    @Override
    public Identifier reserveIdentifier(Session session, String scope,
            IdentifierFormat format) throws InvalidToken, ServiceFailure,
            NotAuthorized, InvalidRequest, NotImplemented {
        throw new NotImplemented("4191", "Client does not implement this method.");
    }
    */

    /* TODO: Remove
    @Override
    public Identifier reserveIdentifier(Session session, String scope)
            throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
            NotImplemented {
        throw new NotImplemented("4191", "Client does not implement this method.");
    }
    */

    /* TODO: Remove
    @Override
    public Identifier reserveIdentifier(Session session) throws InvalidToken,
            ServiceFailure, NotAuthorized, InvalidRequest, NotImplemented {
        throw new NotImplemented("4191", "Client does not implement this method.");
    }
    */

    @Override
    public boolean assertRelation(Session session, Identifier pidOfSubject,
            String relationship, Identifier pidOfObject) throws InvalidToken,
            ServiceFailure, NotAuthorized, NotFound, InvalidRequest,
            NotImplemented {
        throw new NotImplemented("4221", "Client does not implement this method.");
    }



    /**
     * set the access perms for a document
     *
     * @param session
     * @param id
     * @param principal
     * @param permission
     * @param permissionType
     * @param permissionOrder
     *
    @Override
    public boolean setAccess(Session session, Identifier id, String principal,
            String permission, String permissionType, String permissionOrder)
            throws InvalidToken, ServiceFailure, NotFound, NotAuthorized, NotImplemented, InvalidRequest {
        // TODO: this method assumes an access control model that is not finalized, refactor when it is

        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_SESSION);

        url.addNonEmptyParamPair("guid", id.getValue());
        url.addNonEmptyParamPair("principal", principal);
        url.addNonEmptyParamPair("permission", permission);
        url.addNonEmptyParamPair("permissionType", permissionType);
        url.addNonEmptyParamPair("permissionOrder", permissionOrder);
        url.addNonEmptyParamPair("op", "setaccess");
        url.addNonEmptyParamPair("setsystemmetadata", "true");


        if (session == null) {
            session = new Session();
            session.setSubject(new Subject());
            session.getSubject().setValue("public");
        }
        url.addNonEmptyParamPair("sessionid", session.getSubject().getValue());

        RestClient client = new RestClient();
        client.setHeader("session", session.getSubject().getValue());

        HttpResponse hr = null;
        try {
            hr = client.doPostRequest(url.getUrl(), null);
            int statusCode = hr.getStatusLine().getStatusCode();

            if (statusCode != HttpURLConnection.HTTP_OK) {
                throw new ServiceFailure("1000", "Error setting access on document");
            }
        } catch (ClientProtocolException e) {
            throw recastClientSideExceptionToServiceFailure(e);
        } catch (IOException e) {
            throw recastClientSideExceptionToServiceFailure(e);
        }

        return true;
    }
    */


    public Identifier setOwner(Session session, Identifier pid, Subject userId) 
    throws InvalidToken, ServiceFailure, NotFound, NotAuthorized, NotImplemented, InvalidRequest
    {
   	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING);
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("pid", pid, Identifier.class);
    		mpe.addFilePart("userId", userId, Subject.class);
    	} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(true, verbose);
        client.setHeader("session", session.getSubject().getValue());

		InputStream is = null;
		try {
			is = client.doPutRequest(url.getUrl(),mpe);
		} catch (InvalidCredentials e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (IdentifierNotUnique e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (InvalidSystemMetadata e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (AuthenticationTimeout e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedMetadataType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IllegalStateException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (HttpException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		}
        return (Identifier) deserializeServiceType(Identifier.class,is);
    }

    /* TODO: Remove - unneeded
    @Override
    public Subject newAccount(String username, String password) throws IdentifierNotUnique, InvalidCredentials {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    */

    /**
     * deserialize an InputStream to an ObjectLocationList object
     * @param is
     * @return
     * @throws ServiceFailure 
     */
    protected ObjectLocationList deserializeResolve(InputStream is)
            throws ServiceFailure {
        return (ObjectLocationList) deserializeServiceType(ObjectLocationList.class, is);
    }

    @Override
    public boolean isAuthorized(Session session, Identifier pid, Event operation) throws ServiceFailure, InvalidToken, NotFound, NotAuthorized, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean setAccessPolicy(Session session, Identifier pid, AccessPolicy accessPolicy)
    throws InvalidToken, NotFound, NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /* TODO: Remove - unneeded
    @Override
    public Session login(String user, String password, AuthType type) throws InvalidCredentials, AuthenticationTimeout, NotImplemented, InvalidRequest, ServiceFailure {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    */

    /* TODO: Remove - unneeded
    @Override
    public Session getSession(X509Extension cert) throws InvalidCredentials, AuthenticationTimeout, ServiceFailure, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    */

    /* TODO: Remove - unneeded
    @Override
    public Subject newAccount(String username, String password, AuthType type) throws ServiceFailure, IdentifierNotUnique, InvalidCredentials, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    */

    /* TODO: Remove - unneeded
    @Override
    public boolean verifyToken(Session session) throws ServiceFailure, NotAuthorized, NotImplemented, InvalidToken, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    */

    /* TODO: Remove - unneeded
    @Override
    public boolean mapIdentity(Session session1, Session session2) throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    */

    @Override
    public boolean createGroup(Session session, Subject groupName) throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, NotImplemented, InvalidRequest, IdentifierNotUnique {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /* TODO: Remove - unneeded
    @Override
    public Identifier createGroup(Session session, Identifier groupName, List<Identifier> members) throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    */

    /**
     * Find the base URL for a Node based on the Node's identifier as it was registered with the
     * Coordinating Node.
     * @param nodeId the identifier of the node to look up
     * @return the base URL of the node's service endpoints
     */
    public String lookupNodeBaseUrl(String nodeId) {
        String nodeBaseUrl = nodeMap.get(nodeId);
        return nodeBaseUrl;
    }

    /**
     * Find the node identifier for a Node based on the base URL that is used to access its services
     * by looing up the registration for the node at the Coordinating Node.
     * @param nodeBaseUrl the base url for Node service access
     * @return the identifier of the Node
     */
    public String lookupNodeId(String nodeBaseUrl) {
        String nodeId = "";
        for (String key : nodeMap.keySet()) {
            if (nodeBaseUrl.equals(nodeMap.get(key))) {
                // We have a match, so record it and break
                nodeId = key;
                break;
            }
        }

        return nodeId;
    }

    /**
     * Initialize the map of nodes (paids of NodeId/NodeUrl) by getting the map from the CN
     * and parsing the XML, storing the node information in the nodeMap map. These values
     * are used later to look up a node's URL based on its ID, or its ID based on its URL.
     * @throws IOException
     * @throws ParserConfigurationException
     * @throws SAXException
     * @throws XPathExpressionException
     */
    private void initializeNodeMap() throws IOException, XPathExpressionException, SAXException, ParserConfigurationException {
        StringBuffer cnUrl = new StringBuffer(this.getNodeBaseServiceUrl());
        if (!cnUrl.toString().endsWith("/")) {
            cnUrl.append("/");
        }
        cnUrl.append("node");
        URL url;
        url = new URL(cnUrl.toString());
        InputStream is = url.openStream();
        nodeMap = NodeListParser.parseNodeListFile(is);
    }
    
    /**
     * List the registered object formats from the Coordinating Node
     * 
     * @return objectFormatList - the authoritative list of object formats
     * from the coordinating node
     * @throws ServiceFailure 
     * @throws NotFound 
     * @throws InsufficientResources 
     * @throws NotImplemented 
     * @throws InvalidRequest 
     */
    public ObjectFormatList listFormats() 
      throws ServiceFailure, NotFound, InsufficientResources, NotImplemented, 
      InvalidRequest {
      
    	// build the REST URL to call
    	ObjectFormatList objectFormatList = null;
    	D1Url d1Url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_FORMATS);
    	D1RestClient restClient = new D1RestClient(true, verbose);
    	
    	InputStream is = null;
    	  
    	try {
	      // do the request
    		is = restClient.doGetRequest(d1Url.getUrl());
      
    	} catch (NotFound e) {
      	throw new NotFound("4843", "The object formats collection " + 
      		"could not be found at this node - " + 
        	e.getClass() + ": " + e.getMessage());
        
      } catch (InvalidToken e) {
      	throw new ServiceFailure("4841", "Unexpected exception from the service - " + 
        	e.getClass() + ": " + e.getMessage());
        
      } catch (ServiceFailure e) {
      	throw new ServiceFailure("4841", "Unexpected exception from the service - " + 
        	e.getClass() + ": " + e.getMessage());
        
      } catch (NotAuthorized e) {
      	throw new ServiceFailure("4841", "Unexpected exception from the service - " + 
        	e.getClass() + ": " + e.getMessage());
        
      } catch (IdentifierNotUnique e) {
      	throw new ServiceFailure("4841", "Unexpected exception from the service - " + 
        	e.getClass() + ": " + e.getMessage());
        
      } catch (UnsupportedType e) {
      	throw new ServiceFailure("4841", "Unexpected exception from the service - " + 
        	e.getClass() + ": " + e.getMessage());
        
      } catch (UnsupportedQueryType e) {
        	throw new ServiceFailure("4846", "Unexpected exception from the service - " +
            	e.getClass() + ": " + e.getMessage());

      } catch (InsufficientResources e) {
      	throw new InsufficientResources("4844", "The object formats collection " + 
        	"could not be found at this node - " + 
          e.getClass() + ": " + e.getMessage());
          
      } catch (InvalidSystemMetadata e) {
      	throw new ServiceFailure("4841", "Unexpected exception from the service - " + 
        		e.getClass() + ": " + e.getMessage());
        
      } catch (NotImplemented e) {
      	throw new NotImplemented("4840", "the service is not implemented - " + 
        	e.getClass() + ": " + e.getMessage());
        
      } catch (InvalidCredentials e) {
      	throw new ServiceFailure("4841", "Unexpected exception from the service - " + 
        	e.getClass() + ": " + e.getMessage());
        
      } catch (InvalidRequest e) {
      	throw new InvalidRequest("4842", "The request was invalid - " + 
        	e.getClass() + ": " + e.getMessage());
                
      } catch (IllegalStateException e) {
      	throw new ServiceFailure("4841", "Unexpected exception from the service - " + 
        	e.getClass() + ": " + e.getMessage());
                
      } catch (AuthenticationTimeout e) {
      	throw new ServiceFailure("4841", "Unexpected exception from the service - " + 
        	e.getClass() + ": " + e.getMessage());
        
      } catch (ClientProtocolException e) {
      	throw new ServiceFailure("4841", "Unexpected exception from the service - " + 
        		e.getClass() + ": " + e.getMessage());
        
      } catch (UnsupportedMetadataType e) {
      	throw new ServiceFailure("4841", "Unexpected exception from the service - " + 
        	e.getClass() + ": " + e.getMessage());
        
      } catch (IOException e) {
      	throw new ServiceFailure("4841", "Unexpected exception from the service - " + 
        	e.getClass() + ": " + e.getMessage());
        
      } catch (HttpException e) {
      	throw new ServiceFailure("4841", "Unexpected exception from the service - " + 
        	e.getClass() + ": " + e.getMessage());
        
      }
    	
      // deserialize the object format list
      try {
	      
      	objectFormatList = 
      		TypeMarshaller.unmarshalTypeFromStream(ObjectFormatList.class, is);
      
      } catch (IOException e) {
      	throw new ServiceFailure("4841", "Unexpected exception from the service - " + 
      		e.getClass() + ": " + e.getMessage());
      
      } catch (InstantiationException e) {
       	throw new ServiceFailure("4841", "Unexpected exception from the service - " + 
      		e.getClass() + ": " + e.getMessage());
      
      } catch (IllegalAccessException e) {
       	throw new ServiceFailure("4841", "Unexpected exception from the service - " + 
      		e.getClass() + ": " + e.getMessage());
           
      } catch (JiBXException e) {
       	throw new ServiceFailure("4841", "Unexpected exception from the service - " + 
      		e.getClass() + ": " + e.getMessage());
     
      }
    	
      return objectFormatList;
    }
    
    /**
     * Get the object format from the object format list based on the given
     * object format identifier
     * 
     * @param fmtid - the object format identifier
     * @return objectFormat - the requested object format from the registered
     *                        object format list
     * @throws ServiceFailure 
     * @throws NotFound 
     * @throws InsufficientResources 
     * @throws NotImplemented 
     * @throws InvalidRequest 
     */
    public ObjectFormat getFormat(ObjectFormatIdentifier fmtid)
      throws ServiceFailure, NotFound, InsufficientResources, NotImplemented, 
      InvalidRequest {
			
    	ObjectFormat objectFormat = null;
    	
    	// build the REST URL to call
    	D1Url d1Url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_FORMATS);
    	D1RestClient restClient = new D1RestClient(true, verbose);
    	d1Url.addNextPathElement(fmtid.getValue());
    	
    	InputStream is = null;
      
    	try {
    		// do the request
	      is = restClient.doGetRequest(d1Url.getUrl());
      
    	} catch (InvalidToken e) {
      	throw new NotFound("4848", "The format specified by " +
      			fmtid.getValue() +
        		"does not exist at this node - " + 
          	e.getClass() + ": " + e.getMessage());

      } catch (NotAuthorized e) {
      	throw new ServiceFailure("4846", "Unexpected exception from the service - " +
          	e.getClass() + ": " + e.getMessage());

      } catch (IdentifierNotUnique e) {
      	throw new ServiceFailure("4846", "Unexpected exception from the service - " +
          	e.getClass() + ": " + e.getMessage());

      } catch (UnsupportedType e) {
      	throw new ServiceFailure("4846", "Unexpected exception from the service - " +
          	e.getClass() + ": " + e.getMessage());

      } catch (UnsupportedQueryType e) {
        	throw new ServiceFailure("4846", "Unexpected exception from the service - " +
            	e.getClass() + ": " + e.getMessage());

      } catch (InvalidSystemMetadata e) {
      	throw new ServiceFailure("4846", "Unexpected exception from the service - " +
          	e.getClass() + ": " + e.getMessage());

      } catch (InvalidCredentials e) {
      	throw new ServiceFailure("4846", "Unexpected exception from the service - " +
          	e.getClass() + ": " + e.getMessage());

      } catch (IllegalStateException e) {
      	throw new ServiceFailure("4846", "Unexpected exception from the service - " +
          	e.getClass() + ": " + e.getMessage());

      } catch (AuthenticationTimeout e) {
      	throw new ServiceFailure("4846", "Unexpected exception from the service - " +
          	e.getClass() + ": " + e.getMessage());

      } catch (ClientProtocolException e) {
      	throw new ServiceFailure("4846", "Unexpected exception from the service - " +
          	e.getClass() + ": " + e.getMessage());

      } catch (UnsupportedMetadataType e) {
      	throw new ServiceFailure("4846", "Unexpected exception from the service - " +
          	e.getClass() + ": " + e.getMessage());

      } catch (IOException e) {
      	throw new ServiceFailure("4846", "Unexpected exception from the service - " +
          	e.getClass() + ": " + e.getMessage());

      } catch (HttpException e) {
      	throw new ServiceFailure("4846", "Unexpected exception from the service - " +
          	e.getClass() + ": " + e.getMessage());

      }
      
      // deserialize the object format
    	try {
	      
    		objectFormat = 
	      	TypeMarshaller.unmarshalTypeFromStream(ObjectFormat.class, is);
      
    	} catch (IOException e) {
      	throw new ServiceFailure("4846", "Unexpected exception from the service - " + 
      		e.getClass() + ": " + e.getMessage());
      
      } catch (InstantiationException e) {
       	throw new ServiceFailure("4846", "Unexpected exception from the service - " + 
      		e.getClass() + ": " + e.getMessage());
      
      } catch (IllegalAccessException e) {
       	throw new ServiceFailure("4846", "Unexpected exception from the service - " + 
      		e.getClass() + ": " + e.getMessage());
           
      } catch (JiBXException e) {
       	throw new ServiceFailure("4846", "Unexpected exception from the service - " + 
      		e.getClass() + ": " + e.getMessage());
     
      }

    	return objectFormat;
    	
    }
    
    @Override
    public NodeList listNodes() throws NotImplemented, ServiceFailure {

        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_NODE);

        /* FIXME
        if (session == null) {
            session = new Session();
            session.setSubject(new Subject());
            session.getSubject().setValue("public");
        }
        url.addNonEmptyParamPair("sessionid", session.getSubject().getValue());
        */

        D1RestClient client = new D1RestClient(true, verbose);
        // FIXME
        //client.setHeader("session", session.getSubject().getValue());

        InputStream is = null;
        try {
            is = client.doGetRequest(url.getUrl());
        } catch (IdentifierNotUnique e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
        } catch (UnsupportedType e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
        } catch (InsufficientResources e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
        } catch (InvalidSystemMetadata e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
        } catch (InvalidCredentials e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
        } catch (AuthenticationTimeout e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
        } catch (UnsupportedMetadataType e) {
            throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (NotFound ex) {
            throw recastClientSideExceptionToServiceFailure(ex);
        } catch (InvalidToken ex) {
            throw recastClientSideExceptionToServiceFailure(ex);
        } catch (NotAuthorized ex) {
            throw recastClientSideExceptionToServiceFailure(ex);
        } catch (InvalidRequest ex) {
        	// TODO: fix swallowed exception.  throw, recast, or explain in comment :-) 
        } catch (ClientProtocolException e) {
            throw recastClientSideExceptionToServiceFailure(e);
        } catch (IllegalStateException e) {
            throw recastClientSideExceptionToServiceFailure(e);
        } catch (IOException e) {
            throw recastClientSideExceptionToServiceFailure(e);
        } catch (HttpException e) {
            throw recastClientSideExceptionToServiceFailure(e);
        }

        try {
            return TypeMarshaller.unmarshalTypeFromStream(NodeList.class, is);
        } catch (Exception e) {
            throw new ServiceFailure("4801",
                    "Could not deserialize the node list: "
                    + e.getMessage());
        }
    }

//    @Override
    public boolean addNodeCapabilities(Session session, NodeReference nodeid, Node node) throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, NotFound {
        throw new UnsupportedOperationException("Not supported yet.");
    }

//    @Override
    public Identifier register(Session session, Node node) throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, IdentifierNotUnique {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Log getLogRecords(Session session, Date fromDate, Date toDate, Event event) throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
//    @Override
    public boolean registerSystemMetadata(Session session, Identifier pid, SystemMetadata sysmeta) throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, InvalidSystemMetadata {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Checksum getChecksum(Session session, Identifier pid) throws NotImplemented, ServiceFailure, NotFound, NotAuthorized, InvalidRequest, InvalidToken {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean removeGroupMembers(Session session, Subject groupName, SubjectList members) throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, NotImplemented, InvalidRequest{
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean addGroupMembers(Session session, Subject groupName, SubjectList members) throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, NotImplemented, InvalidRequest{
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean confirmMapIdentity(Session session, Subject subject) throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, NotImplemented, InvalidRequest{
        throw new UnsupportedOperationException("Not supported yet.");
    }

    
    @Override
    public boolean mapIdentity(Session session, Subject subject) 
    throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
    NotImplemented, InvalidRequest
    {
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING);
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("subject", subject, Subject.class);
    	} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(true, verbose);
        client.setHeader("session", session.getSubject().getValue());

		InputStream is = null;
		try {
			is = client.doPostRequest(url.getUrl(),mpe);
		} catch (InvalidCredentials e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (IdentifierNotUnique e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (InvalidSystemMetadata e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (AuthenticationTimeout e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedMetadataType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (NotFound e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IllegalStateException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (HttpException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		}
		return true;
    }

    
    @Override
    public SubjectList listSubjects(Session session, String query, Integer start, Integer count) 
    throws ServiceFailure, InvalidToken, NotAuthorized, NotImplemented 
    {
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNTS);
    	url.addNonEmptyParamPair("query", query);
    	url.addNonEmptyParamPair("start", start);
    	url.addNonEmptyParamPair("count", count);
    	
		D1RestClient client = new D1RestClient(true, verbose);
        client.setHeader("session", session.getSubject().getValue());

		InputStream is = null;
		try {
			is = client.doGetRequest(url.getUrl());
		} catch (InvalidRequest e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (InvalidCredentials e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (IdentifierNotUnique e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (InvalidSystemMetadata e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (AuthenticationTimeout e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedMetadataType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (NotFound e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IllegalStateException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (HttpException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		}
		return (SubjectList) deserializeServiceType(SubjectList.class,is);
    }

    
//    @Override
    public SubjectList getSubjectInfo(Session session, Subject subject) 
    throws ServiceFailure, InvalidRequest, NotAuthorized, NotImplemented
    {
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNTS);
    	url.addNextPathElement(subject.getValue());

		D1RestClient client = new D1RestClient(true, verbose);
        client.setHeader("session", session.getSubject().getValue());

		InputStream is = null;
		try {
			is = client.doGetRequest(url.getUrl());
		} catch (ServiceFailure e) {
			throw e;
		} catch (InvalidRequest e) {
			throw e;
		} catch (NotAuthorized e) {
			throw e;
		} catch (NotImplemented e) {
			throw e;
		} catch (BaseException e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IllegalStateException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (HttpException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		}
		return (SubjectList) deserializeServiceType(SubjectList.class,is);
    }
 
    
    @Override
    public boolean verifyAccount(Session session, Subject subject) 
    throws ServiceFailure, NotAuthorized, NotImplemented, InvalidToken, InvalidRequest 
    {
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNTS);
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("subject", subject, Subject.class);
    	} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}
        if (session == null) {
            session = new Session();
            session.setSubject(new Subject());
            session.getSubject().setValue("public");
        }
        url.addNonEmptyParamPair("sessionid", session.getSubject().getValue());

        D1RestClient client = new D1RestClient(true, verbose);
        client.setHeader("session", session.getSubject().getValue());

		InputStream is = null;
		try {
			is = client.doPostRequest(url.getUrl(),mpe);
		} catch (InvalidCredentials e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (IdentifierNotUnique e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (InvalidSystemMetadata e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (AuthenticationTimeout e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedMetadataType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (NotFound e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IllegalStateException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (HttpException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		}
		return true;
    }
    
    
    @Override
    public Subject updateAccount(Session session, Person person) 
    throws ServiceFailure, IdentifierNotUnique, InvalidCredentials, NotImplemented, InvalidRequest 
    {
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_POLICY);
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("person", person, Person.class);
    	} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}
        if (session == null) {
            session = new Session();
            session.setSubject(new Subject());
            session.getSubject().setValue("public");
        }
        url.addNonEmptyParamPair("sessionid", session.getSubject().getValue());

		D1RestClient client = new D1RestClient(true, verbose);
        client.setHeader("session", session.getSubject().getValue());

		InputStream is = null;
		try {
			is = client.doPutRequest(url.getUrl(),mpe);
		} catch (NotAuthorized e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (InvalidSystemMetadata e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (AuthenticationTimeout e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedMetadataType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (InvalidToken e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (NotFound e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IllegalStateException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (HttpException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		}
		return (Subject) deserializeServiceType(Subject.class,is);
    }
    
    
    @Override
    public Subject registerAccount(Session session, Person person) 
    throws ServiceFailure, IdentifierNotUnique, InvalidCredentials, NotImplemented, InvalidRequest 
    {
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_POLICY);
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("person", person, Person.class);
    	} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}
        if (session == null) {
            session = new Session();
            session.setSubject(new Subject());
            session.getSubject().setValue("public");
        }
        url.addNonEmptyParamPair("sessionid", session.getSubject().getValue());

		D1RestClient client = new D1RestClient(true, verbose);
        client.setHeader("session", session.getSubject().getValue());

		InputStream is = null;
		try {
			is = client.doPostRequest(url.getUrl(),mpe);
		} catch (NotAuthorized e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (InvalidSystemMetadata e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (AuthenticationTimeout e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedMetadataType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (InvalidToken e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (NotFound e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IllegalStateException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (HttpException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		}
		return (Subject) deserializeServiceType(Subject.class,is);
    }
    
    @Override
    public boolean setReplicationPolicy(Session session, Identifier pid, ReplicationPolicy policy) 
    throws ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest 
    {
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_POLICY);
    	url.addNextPathElement(pid.getValue());
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("policy", policy, ReplicationPolicy.class);
    	} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}
        if (session == null) {
            session = new Session();
            session.setSubject(new Subject());
            session.getSubject().setValue("public");
        }
        url.addNonEmptyParamPair("sessionid", session.getSubject().getValue());

		D1RestClient client = new D1RestClient(true, verbose);
        client.setHeader("session", session.getSubject().getValue());

		InputStream is = null;
		try {
			is = client.doPutRequest(url.getUrl(),mpe);
		} catch (IdentifierNotUnique e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (InvalidSystemMetadata e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (InvalidCredentials e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (AuthenticationTimeout e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedMetadataType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (InvalidToken e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (NotFound e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IllegalStateException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (HttpException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		}
		return true;
    }
  
    
    @Override
    public boolean setReplicationStatus(Session session, Identifier pid, ReplicationStatus status) 
    throws ServiceFailure, NotImplemented, InvalidToken, NotAuthorized, InvalidRequest, NotFound 
    {
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_NOTIFY);
    	url.addNextPathElement(pid.getValue());
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("status", status, ReplicationStatus.class);
    	} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}
        if (session == null) {
            session = new Session();
            session.setSubject(new Subject());
            session.getSubject().setValue("public");
        }
        url.addNonEmptyParamPair("sessionid", session.getSubject().getValue());

		D1RestClient client = new D1RestClient(true, verbose);
        client.setHeader("session", session.getSubject().getValue());

		InputStream is = null;
		try {
			is = client.doPostRequest(url.getUrl(),mpe);
		} catch (IdentifierNotUnique e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (InvalidSystemMetadata e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (InvalidCredentials e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (AuthenticationTimeout e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (UnsupportedMetadataType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IllegalStateException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (HttpException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		}
		return true;
    }
}
