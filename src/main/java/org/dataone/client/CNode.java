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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.security.cert.X509Extension;
import java.util.List;
import org.apache.http.HttpException;
import org.apache.http.client.ClientProtocolException;
import org.dataone.mimemultipart.SimpleMultipartEntity;

import org.dataone.service.Constants;
import org.dataone.service.D1Url;
import org.dataone.service.EncodingUtilities;
import org.dataone.service.cn.CoordinatingNodeAuthentication;
import org.dataone.service.cn.CoordinatingNodeAuthorization;
import org.dataone.service.cn.CoordinatingNodeCrud;
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
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.types.AccessPolicy;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.AuthType;
import org.dataone.service.types.Event;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.IdentifierFormat;
import org.dataone.service.types.ObjectList;
import org.dataone.service.types.ObjectLocationList;
import org.dataone.service.types.Principal;
import org.dataone.service.types.SystemMetadata;
import org.jibx.runtime.JiBXException;

/**
 * CNode represents a DataONE Coordinating Node, and allows calling classes to
 * execute CN services.
 */

public class CNode extends D1Node implements CoordinatingNodeCrud, CoordinatingNodeAuthorization, CoordinatingNodeAuthentication {

    /**
     * Construct a Coordinating Node, passing in the base url for node services.
     * @param nodeBaseServiceUrl base url for constructing service endpoints.
     */
    public CNode(String nodeBaseServiceUrl) {
        super(nodeBaseServiceUrl);
    }

    public ObjectList search(AuthToken token,String paramString) 
    throws NotAuthorized, InvalidRequest,
    NotImplemented, ServiceFailure, InvalidToken {

    	String resource = Constants.RESOURCE_OBJECTS;
    	String paramAdditions = "";
    	if (!paramString.contains("qt=solr")) {
    		paramAdditions = "qt=solr&";
    	}
    	if (!paramString.contains("pageSize=\\d+")) {
    		paramAdditions += "pageSize=200&";
    	}
    	if (!paramString.contains("start=\\d+")) {
    		paramAdditions += "start=0&";
    	}

    	String paramsComplete = paramAdditions + paramString;
    	
    	// clean up paramsComplete string
    	if (paramsComplete.endsWith("&"))
    		paramsComplete = paramsComplete.substring(0, paramsComplete.length()-1);



    	InputStream is;
    	try {
    		is = handleHttpGet(token,resource,paramsComplete);
    	} catch (NotFound eNF) {
    		// convert notfound error to service failure
    		// because it is not valid in the listObjects case
    		throw new ServiceFailure("1000", "Method threw unexpected exception: " + eNF.getMessage());
    	}

    	try {
    		return deserializeObjectList(is);
    	} catch (JiBXException e) {
    		throw new ServiceFailure("500",
    				"Could not deserialize the ObjectList: " + e.getMessage());
    	}
    }

    
    @Override
    public InputStream get(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        return super.get(token, guid);
    }

    @Override
    public SystemMetadata getSystemMetadata(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            InvalidRequest, NotImplemented {
        return super.getSystemMetadata(token, guid);
    }

    @Override
    public ObjectLocationList resolve(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            InvalidRequest, NotImplemented {
        String resource = Constants.RESOURCE_RESOLVE + "/" + EncodingUtilities.encodeUrlPathSegment(guid.getValue());
        InputStream is = null;
        ResponseData rd = sendRequest(token, resource, Constants.GET, null, null, null, "text/xml");

        int code = rd.getCode();
        if (code != HttpURLConnection.HTTP_OK) {
            InputStream errorStream = rd.getErrorStream();
            try {
                deserializeAndThrowException(code, errorStream);
            } catch (InvalidToken e) {
                throw e;
            } catch (ServiceFailure e) {
                throw e;
            } catch (NotAuthorized e) {
                throw e;
            } catch (NotFound e) {
                throw e;
            } catch (NotImplemented e) {
                throw e;
            } catch (BaseException e) {
                throw new ServiceFailure("1000",
                        "Method threw improper exception: " + e.getMessage());
            }
        } else {
            is = rd.getContentStream();
        }
        try {
            return deserializeResolve(is);
        } catch (Exception e) {
            throw new ServiceFailure("1090",
                    "Could not deserialize the systemMetadata: "
                            + e.getMessage());
        }
    }
    /**
     * create both a system metadata resource and science metadata resource with
     * the specified guid
     */
    @Override
    public Identifier create(AuthToken token, Identifier guid,
            InputStream object, SystemMetadata sysmeta) throws InvalidToken,
            ServiceFailure, NotAuthorized, IdentifierNotUnique,
            UnsupportedType, InsufficientResources, InvalidSystemMetadata,
            NotImplemented {


        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
        url.addNextPathElement(guid.getValue());
        if (token != null) {
            url.addNonEmptyParamPair("sessionid", token.getToken());
        }

        SimpleMultipartEntity mpe = new SimpleMultipartEntity();

        // Coordinating Nodes must maintain systemmetadata of all object on dataone
        // however Coordinating nodes do not house Science Data only Science Metadata
        // Thus, the inputstream for an object may be null
        // so deal with it here ...
        // and this is how CNs are different from MNs
        // because if object is null on an MN, we should throw an exception
        
        if (object == null) {
            // XXX a bit confusing with these method signatures,
            // one takes the name first
            // the other takes the name second???
            mpe.addFilePart("object", "");
        } else {
            mpe.addFilePart(object, "object");
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            serializeServiceType(SystemMetadata.class, sysmeta, baos);
        } catch (JiBXException e) {
            throw new ServiceFailure("1090",
                    "Could not serialize the systemMetadata: "
                    + e.getMessage());
        }
//      	mpe.addFilePart("systemmetadata",baos.toString());
        mpe.addFilePart("sysmeta", baos.toString());

        D1RestClient client = new D1RestClient();
        InputStream is = null;

        try {
            is = client.doPostRequest(url.getUrl(), mpe);
        } catch (NotFound e) {
            throw new ServiceFailure("1090", e.getClass().getSimpleName() + ": " + e.getDetail_code() + ": "+ e.getDescription());
        } catch (InvalidCredentials e) {
            throw new ServiceFailure("1090", e.getClass().getSimpleName() + ": " + e.getDetail_code() + ": "+ e.getDescription());
        } catch (InvalidRequest e) {
            throw new ServiceFailure("1090", e.getClass().getSimpleName() + ": "  + e.getDetail_code() + ": "+ e.getDescription());
        } catch (AuthenticationTimeout e) {
            throw new ServiceFailure("1090", e.getClass().getSimpleName() + ": "  + e.getDetail_code() + ": "+ e.getDescription());
        } catch (UnsupportedMetadataType e) {
            throw new ServiceFailure("1090", e.getClass().getSimpleName() + ": " + e.getDetail_code() + ": "+ e.getDescription());
        } catch (ClientProtocolException e) {
            throw new ServiceFailure("1090", e.getClass().getSimpleName() + ": " + e.getMessage());
        } catch (IllegalStateException e) {
            throw new ServiceFailure("1090", e.getClass().getSimpleName() + ": " + e.getMessage());
        } catch (IOException e) {
            throw new ServiceFailure("1090", e.getClass().getSimpleName() + ": " + e.getMessage());
        } catch (HttpException e) {
            throw new ServiceFailure("1090", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        try {
            // 		System.out.println(IOUtils.toString(is));
            return (Identifier) deserializeServiceType(Identifier.class, is);
        } catch (Exception e) {
            throw new ServiceFailure("1090",
                    "Could not deserialize the Identifier: " + e.getMessage());
        }
// //       return handleCreateOrUpdate(token, guid, object, sysmeta, null, "create");
    }
    
    @Override
    public Identifier reserveIdentifier(AuthToken token, String scope,
            IdentifierFormat format) throws InvalidToken, ServiceFailure,
            NotAuthorized, InvalidRequest, NotImplemented {
        throw new NotImplemented("4191", "Client does not implement this method.");
    }

    @Override
    public Identifier reserveIdentifier(AuthToken token, String scope)
            throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
            NotImplemented {
        throw new NotImplemented("4191", "Client does not implement this method.");
    }

    @Override
    public Identifier reserveIdentifier(AuthToken token, IdentifierFormat format)
            throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
            NotImplemented {
        throw new NotImplemented("4191", "Client does not implement this method.");
    }

    @Override
    public Identifier reserveIdentifier(AuthToken token) throws InvalidToken,
            ServiceFailure, NotAuthorized, InvalidRequest, NotImplemented {
        throw new NotImplemented("4191", "Client does not implement this method.");
    }

    @Override
    public boolean assertRelation(AuthToken token, Identifier subjectId,
            String relationship, Identifier objectId) throws InvalidToken,
            ServiceFailure, NotAuthorized, NotFound, InvalidRequest,
            NotImplemented {
        throw new NotImplemented("4221", "Client does not implement this method.");
    }

    /**
     * login and get an AuthToken
     *
     * @param username
     * @param password
     * @return
     * @throws ServiceFailure
     */
    @Override
    public AuthToken login(String username, String password)
            throws InvalidCredentials, AuthenticationTimeout, ServiceFailure, NotImplemented, InvalidRequest {
        // TODO: reassess the exceptions thrown here.  Look at the Authentication interface.
        // TODO: this method assumes an access control model that is not finalized, refactor when it is
        String postData = "username=" + username + "&password=" + password;
        String params = "qformat=xml&op=login";
        String resource = Constants.RESOURCE_SESSION + "/";

        ResponseData rd = sendRequest(null, resource, Constants.POST, params, null,
                new ByteArrayInputStream(postData.getBytes()), null);
        String sessionid = null;

        int code = rd.getCode();
        if (code != HttpURLConnection.HTTP_OK) { // deal with the error
            // TODO: detail codes are wrong, and exception is the wrong one too I think
            throw new ServiceFailure("1000", "Error logging in.");
        } else {
            try {
                // TODO: use IOUtils to get the string, as this code is error prone
                InputStream is = rd.getContentStream();
                byte[] b = new byte[1024];
                int numread = is.read(b, 0, 1024);
                StringBuffer sb = new StringBuffer();
                while (numread != -1) {
                    sb.append(new String(b, 0, numread));
                    numread = is.read(b, 0, 1024);
                }
                String response = sb.toString();
                //String response = IOUtils.toString(is);


                int successIndex = response.indexOf("<sessionId>");
                if (successIndex != -1) {
                    sessionid = response.substring(
                            response.indexOf("<sessionId>")
                                    + "<sessionId>".length(),
                            response.indexOf("</sessionId>"));
                } else {
                    // TODO: wrong exception thrown, wrong detail code?
                    throw new ServiceFailure("1000", "Error authenticating: "
                            + response.substring(response.indexOf("<error>")
                                    + "<error>".length(),
                                    response.indexOf("</error>")));
                }
            } catch (Exception e) {
                throw new ServiceFailure("1000",
                        "Error getting response from metacat: "
                                + e.getMessage());
            }
        }

        return new AuthToken(sessionid);
    }

    /**
     * set the access perms for a document
     *
     * @param token
     * @param id
     * @param principal
     * @param permission
     * @param permissionType
     * @param permissionOrder
     */
    @Override
    public boolean setAccess(AuthToken token, Identifier id, String principal,
            String permission, String permissionType, String permissionOrder)
            throws InvalidToken, ServiceFailure, NotFound, NotAuthorized, NotImplemented, InvalidRequest  {
        // TODO: this method assumes an access control model that is not finalized, refactor when it is
        String params = "guid=" + EncodingUtilities.encodeUrlQuerySegment(id.getValue()) + "&principal=" + principal
                + "&permission=" + permission + "&permissionType="
                + permissionType + "&permissionOrder=" + permissionOrder
                + "&op=setaccess&setsystemmetadata=true";
        String resource = Constants.RESOURCE_SESSION + "/";
        ResponseData rd = sendRequest(token, resource, Constants.POST, params, null, null, null);
        int code = rd.getCode();
        if (code != HttpURLConnection.HTTP_OK) {
            throw new ServiceFailure("1000", "Error setting acces on document");
        }
        return true;
        // TODO: also set the system metadata to the same perms

    }

    @Override
    public Identifier setOwner(AuthToken token, Identifier guid, Principal userId) throws InvalidToken, NotAuthorized, NotFound {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Principal newAccount(String username, String password) throws IdentifierNotUnique, InvalidCredentials {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * deserialize an InputStream to an ObjectLocationList object
     * @param is
     * @return
     * @throws JiBXException
     */
    protected ObjectLocationList deserializeResolve(InputStream is)
                    throws JiBXException {
            return (ObjectLocationList) deserializeServiceType(ObjectLocationList.class, is);
    }

    @Override
    public boolean isAuthorized(AuthToken token, Identifier pid, Event operation) throws ServiceFailure, InvalidToken, NotFound, NotAuthorized, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean setAccess(AuthToken token, Identifier pid, AccessPolicy accessPolicy) throws InvalidToken, ServiceFailure, NotFound, NotAuthorized, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public AuthToken login(String user, String password, AuthType type) throws InvalidCredentials, AuthenticationTimeout, NotImplemented, InvalidRequest, ServiceFailure {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public AuthToken getAuthToken(X509Extension cert) throws InvalidCredentials, AuthenticationTimeout, ServiceFailure, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Principal newAccount(String username, String password, AuthType type) throws ServiceFailure, IdentifierNotUnique, InvalidCredentials, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean verifyToken(AuthToken token) throws ServiceFailure, NotAuthorized, NotImplemented, InvalidToken, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean mapIdentity(AuthToken token1, AuthToken token2) throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Identifier createGroup(AuthToken token, Identifier groupName) throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, NotImplemented, InvalidRequest, IdentifierNotUnique {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Identifier createGroup(AuthToken token, Identifier groupName, List<Identifier> members) throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, NotImplemented, InvalidRequest {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
