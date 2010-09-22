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
import java.net.HttpURLConnection;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.dataone.service.cn.CoordinatingNodeCrud;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.IdentifierFormat;
import org.dataone.service.types.NodeReference;
import org.dataone.service.types.ObjectLocation;
import org.dataone.service.types.ObjectLocationList;
import org.dataone.service.types.SystemMetadata;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * CNode represents a DataONE Coordinating Node, and allows calling classes to
 * execute CN services.
 */
public class CNode extends D1Node implements CoordinatingNodeCrud {

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
        String resource = RESOURCE_RESOLVE + "/" + guid.getValue();
        InputStream is = null;
        ResponseData rd = sendRequest(token, resource, GET, null, null, null, "text/xml");

        ObjectLocationList oll = null;
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
            oll = new ObjectLocationList();
            DocumentBuilderFactory df = DocumentBuilderFactory.newInstance();
            df.setValidating(false);
            try {
                // Extract the list of locations from the XML resolve response,
                // and create ObjectLocation instances for each location
                DocumentBuilder db = df.newDocumentBuilder();
                Document resolveDoc = db.parse(is);
                NodeList nl = resolveDoc.getElementsByTagName("objectLocation");
                for (int i = 0; i < nl.getLength(); i++) {
                    Node olNode = nl.item(i);
                    olNode.normalize();
                    Node idNode = olNode.getFirstChild();
                    String mnId = idNode.getFirstChild().getNodeValue();
                    NodeReference mnRef = new NodeReference();
                    mnRef.setValue(mnId);
                    Node urlNode = olNode.getLastChild();
                    String url = urlNode.getLastChild().getNodeValue();
                    ObjectLocation ol = new ObjectLocation(mnRef, url);
                    oll.add(ol);
                }
            } catch (ParserConfigurationException e) {
                throw new ServiceFailure("1000",
                        "Failed to parse object location list XML: "
                                + e.getMessage());
            } catch (SAXException e) {
                throw new ServiceFailure("1000",
                        "SAX failure while parsing object location list XML: "
                                + e.getMessage());
            } catch (IOException e) {
                throw new ServiceFailure("1000",
                        "Failure to get object location list stream for parsing: "
                                + e.getMessage());
            }

        }
        
        return oll;
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

}
