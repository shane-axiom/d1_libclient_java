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

import com.gc.iotools.stream.is.InputStreamFromOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.commons.io.IOUtils;
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
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.IdentifierFormat;
import org.dataone.service.types.NodeReference;
import org.dataone.service.types.ObjectLocationList;
import org.dataone.service.types.Principal;
import org.dataone.service.types.SystemMetadata;
import org.dataone.service.Constants;
//import org.omg.CosNaming.NamingContextPackage.NotFound;
import org.jibx.runtime.JiBXException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * CNode represents a DataONE Coordinating Node, and allows calling classes to
 * execute CN services.
 */

public class CNode extends D1Node implements CoordinatingNodeCrud, CoordinatingNodeAuthorization {

    /**
     * Construct a Coordinating Node, passing in the base url for node services.
     * @param nodeBaseServiceUrl base url for constructing service endpoints.
     */
    public CNode(String nodeBaseServiceUrl) {
        super(nodeBaseServiceUrl);
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
        String resource = Constants.RESOURCE_RESOLVE + "/" + guid.getValue();
        InputStream is = null;
        ResponseData rd = sendRequest(token, resource, Constants.GET, null, null, null, "text/xml");

        int code = rd.getCode();
        if (code != HttpURLConnection.HTTP_OK) {
            InputStream errorStream = rd.getErrorStream();
            try {
                deserializeAndThrowException(errorStream);
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
    public Identifier create(AuthToken token, Identifier guid,
            final InputStream object, final SystemMetadata sysmeta) throws InvalidToken,
            ServiceFailure, NotAuthorized, IdentifierNotUnique,
            UnsupportedType, InsufficientResources, InvalidSystemMetadata,
            NotImplemented {

        String resource = Constants.RESOURCE_OBJECTS + "/" + guid.getValue();

        final InputStreamFromOutputStream<String> multipartStream = new InputStreamFromOutputStream<String>() {
            @Override
            public String produce(java.io.OutputStream dataSink) throws Exception 
            {
                createMimeMultipart(dataSink, object, sysmeta);
                IOUtils.closeQuietly(dataSink);
                return "Complete";
            }
        };

        ResponseData rd = sendRequest(token, resource, Constants.POST, null,
                "multipart/mixed", multipartStream, null);

        // Handle any errors that were generated
        int code = rd.getCode();
        if (code != HttpURLConnection.HTTP_OK) {
            InputStream errorStream = rd.getErrorStream();
            try {
                byte[] b = new byte[1024];
                int numread = errorStream.read(b, 0, 1024);
                StringBuffer sb = new StringBuffer();
                while (numread != -1) {
                    sb.append(new String(b, 0, numread));
                    numread = errorStream.read(b, 0, 1024);
                }
                deserializeAndThrowException(errorStream);
            } catch (InvalidToken e) {
                throw e;
            } catch (ServiceFailure e) {
                throw e;
            } catch (NotAuthorized e) {
                throw e;
            } catch (IdentifierNotUnique e) {
                throw e;
            } catch (UnsupportedType e) {
                throw e;
            } catch (InsufficientResources e) {
                throw e;
            } catch (InvalidSystemMetadata e) {
                throw e;
            } catch (NotImplemented e) {
                throw e;
            } catch (BaseException e) {
                throw new ServiceFailure("1000",
                        "Method threw improper exception: " + e.getMessage());
            } catch (IOException e) {
                System.out.println("io exception: " + e.getMessage());
            }
        } 
        return guid;
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
            throws InvalidCredentials, AuthenticationTimeout, ServiceFailure {
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
            throws ServiceFailure {
        // TODO: this method assumes an access control model that is not finalized, refactor when it is
        String params = "guid=" + id.getValue() + "&principal=" + principal
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

    @Override
    public boolean verify(AuthToken token) throws NotAuthorized {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isAuthorized(AuthToken token, Identifier guid, String operation) throws InvalidToken, NotFound, NotAuthorized {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    /**
     * deserialize an InputStream to a SystemMetadata object
     * @param is
     * @return
     * @throws JiBXException
     */
    protected ObjectLocationList deserializeResolve(InputStream is)
                    throws JiBXException {
            return (ObjectLocationList) deserializeServiceType(ObjectLocationList.class, is);
    }

}
