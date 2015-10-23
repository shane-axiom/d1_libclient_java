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

package org.dataone.client.rest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.AutoCloseInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.dataone.client.D1Node;
import org.dataone.client.auth.X509Session;
import org.dataone.client.auth.AuthTokenSession;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.exception.NotCached;
import org.dataone.client.utils.ExceptionUtils;
import org.dataone.client.v1.cache.LocalCache;
import org.dataone.configuration.Settings;
import org.dataone.mimemultipart.SimpleMultipartEntity;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.DescribeResponse;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1_1.QueryEngineDescription;
import org.dataone.service.types.v1_1.QueryEngineList;
import org.dataone.service.types.v2.OptionList;
import org.dataone.service.util.BigIntegerMarshaller;
import org.dataone.service.util.Constants;
import org.dataone.service.util.D1Url;
import org.dataone.service.util.DateTimeMarshaller;
import org.dataone.service.util.TypeMarshaller;
import org.jibx.runtime.JiBXException;

/**
 * An abstract node class that contains base functionality shared between
 * Coordinating Node and Member Node implementations.
 *
 * Various methods may set their own timeouts by use of Settings.Configuration properties
 * or by calling setDefaultSoTimeout.  Settings.Configuration properties override
 * any value of the DefaultSoTimeout.  Timeouts are always represented in milliseconds
 *
 * timeout properties recognized:
 * D1Client.D1Node.listObjects.timeout
 * D1Client.D1Node.getLogRecords.timeout
 * D1Client.D1Node.get.timeout
 * D1Client.D1Node.getSystemMetadata.timeout
 *
 */
public abstract class MultipartD1Node implements D1Node {

    protected static org.apache.commons.logging.Log log = LogFactory.getLog(MultipartD1Node.class);

    /** the D1Node instance adapter / connector to the RESTful service endpoints */
    protected MultipartRestClient defaultRestClient;

    /** the latest MRC used to make an API call */
    protected MultipartRestClient latestRestClient;
    protected Session latestSession;

    /** The URL string for the node REST API */
    private String nodeBaseServiceUrl;

    /** The string representation of the NodeReference */
    private NodeReference nodeId;

    /** this represents the defaultSession to be used for establishing the SSL connection */
    protected Session defaultSession;

    /** flag that controls whether or not a local cache is used */
    private boolean useLocalCache = false;

    protected NodeType nodeType;

    
    /**
     * Utility method to be used in finally blocks for methods that have to 
     * close inputStreams.  Contrast with IOUtils.closeQuietly(is).  Closes the 
     * input stream, taking care of null situations, and throwing an appropriate 
     * DataONE exception if there is a problem with closing.
     * @param is
     * @throws ServiceFailure - thrown if is.close throws an IOException
     */
    protected static void closeLoudly(InputStream is) throws ServiceFailure {
        if (is != null) {
            try {
                is.close();
            } catch (IOException e) {
                throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
            }
        }
    }
    
    
    /**
     * API methods should use this method to get the MultipartRestClient to
     * be used for the call.
     * @param sessionFromMethod - this should be the session from the API method parameter
     * @return
     * @throws ServiceFailure 
     */
    protected MultipartRestClient getRestClient(Session sessionFromMethod) throws ServiceFailure {
        if (sessionFromMethod == null) {
            this.latestRestClient = this.defaultRestClient;
            
        } 
        else if (sessionFromMethod instanceof X509Session && ((X509Session)sessionFromMethod).getMultipartRestClient() != null) {
            this.latestRestClient = ((X509Session)sessionFromMethod).getMultipartRestClient();
        } 
        else if (sessionFromMethod instanceof AuthTokenSession && ((AuthTokenSession)sessionFromMethod).getMultipartRestClient() != null) {
           this.latestRestClient = ((AuthTokenSession)sessionFromMethod).getMultipartRestClient();
        }
        else {
            try {
                String subjectString = (sessionFromMethod.getSubject() == null) ? null : sessionFromMethod.getSubject().getValue();
                this.latestRestClient = new HttpMultipartRestClient(subjectString);
            } catch (IOException | ClientSideException e) {
                throw new ServiceFailure("0000", "Error creating MultipartRestClient " +
                        "for API Session parameter: " + e.getLocalizedMessage());
            }
        }
        if (this.latestRestClient.getSession() instanceof X509Session)
            try {
                ((X509Session) this.latestRestClient.getSession()).checkValidity();
            } catch (CertificateExpiredException
                    | CertificateNotYetValidException e) {
                throw new ServiceFailure("0000", "Certificate is expired (or not yet valid): " + e.getLocalizedMessage());
            }
        
        return this.latestRestClient;
    }


    /**
     * Users can set either a default RestClient or default Session that will 
     * build a RestClient configured appropriately from the information in the 
     * Session.
     */
    protected MultipartD1Node(MultipartRestClient client, String nodeBaseServiceUrl, Session session) {
        setNodeBaseServiceUrl(nodeBaseServiceUrl);
        this.defaultRestClient = client;
        this.defaultSession = session;
        this.latestSession = session; // setting the value here to avoid NPEs in getLatestRequestUrl() 
        this.useLocalCache = Settings.getConfiguration().getBoolean("D1Client.useLocalCache",useLocalCache);
    }

    /**
     * Convenience constructor to create a new instance.
     */
    public MultipartD1Node(MultipartRestClient client, String nodeBaseServiceUrl) {
        this(client, nodeBaseServiceUrl, /* Session */ null);
    }
    
    /**
     * Convenience constructor to create a new instance.
     */
    public MultipartD1Node(Session session, String nodeBaseServiceUrl) {
        this(/* MRC */ null, nodeBaseServiceUrl, session);
    }



    /**
     * Convenience constructor to create a new instance using a new DefaultHttpMultipartRestClient
     * (can be retrieved with the (protected) getRestClient()) 
     * This method is meant for backwards compatibility with v1 libclient D1Node class,
     * but it's use is strongly discouraged due to lack of exposure of the rest client.
     */
//    @Deprecated
//    public MultipartD1Node(String nodeBaseServiceUrl, Session session) {
//        this(new HttpMultipartRestClient(session), nodeBaseServiceUrl, session);
//    }



    /**
     * Constructor to create a new instance.
     * @throws ClientSideException 
     * @throws IOException 
     */
    @Deprecated
    public MultipartD1Node(String nodeBaseServiceUrl) throws IOException, ClientSideException {
        setNodeBaseServiceUrl(nodeBaseServiceUrl);
        this.defaultRestClient = new DefaultHttpMultipartRestClient();
        this.defaultSession = null;
        this.useLocalCache = Settings.getConfiguration().getBoolean("D1Client.useLocalCache",useLocalCache);
    }

    // TODO: this constructor should not exist
    // lest we end up with a client that is not attached to a particular node;
    // No code calls it in Java, but it is called by the R client; evaluate if this can change
    /**
     * default constructor needed by some clients.  This constructor will probably
     * go away so don't depend on it.  Use public MultipartD1Node(String nodeBaseServiceUrl) instead.
     */
    @Deprecated
    public MultipartD1Node() {
    }


    /**
     * Retrieve the service URL for this node.  The service URL can be used with
     * knowledge of the DataONE REST API to construct endpoints for each of the
     * DataONE REST services that are available on the node.
     * @return String representing the service URL
     */
    public String getNodeBaseServiceUrl() {
        return this.nodeBaseServiceUrl;
    }

    /**
     * Set the service URL for this node.  The service URL can be used with
     * knowledge of the DataONE REST API to construct endpoints for each of the
     * DataONE REST services that are available on the node.
     * @param nodeBaseServiceUrl String representing the service URL
     */
    public void setNodeBaseServiceUrl(String nodeBaseServiceUrl) {
        if (nodeBaseServiceUrl != null && !nodeBaseServiceUrl.endsWith("/")) {
            nodeBaseServiceUrl = nodeBaseServiceUrl + "/";
        }
        this.nodeBaseServiceUrl = nodeBaseServiceUrl;
    }

    /**
     * @return the nodeId
     */
    public NodeReference getNodeId() {
        return nodeId;
    }

    /**
     * @param nodeId the nodeId to set
     */
    public void setNodeId(NodeReference nodeId) {
        this.nodeId = nodeId;
    }

    public void setNodeType(NodeType nodeType) {
        this.nodeType = nodeType;
    }

    public NodeType getNodeType() {
        return this.nodeType;
    }

    /**
     * Useful for debugging to see what the last call was
     */
    public String getLatestRequestUrl() {
        return this.latestRestClient.getLatestRequestUrl();
    }


    public Date ping() throws NotImplemented, ServiceFailure, InsufficientResources
    {

        // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_MONITOR_PING);

        // we are going to get the date from
        // the headers instead of looking in the message body
        Header[] headers = null;

        try {
            headers = getRestClient(defaultSession).doGetRequestForHeaders(url.getUrl(), null);
        } catch (BaseException be) {
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof InsufficientResources)  throw (InsufficientResources) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        }
        catch (ClientSideException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        }


        // if exception not thrown, and we got this far,
        // then pull the date info from the headers
        Date date = null;
        for (Header header: headers) {
            if (log.isDebugEnabled())
                log.debug(String.format("header: %s = %s",
                        header.getName(),
                        header.getValue() ));
            if (header.getName().equals("Date"))
                date = DateTimeMarshaller.deserializeDateToUTC(header.getValue());
        }
        if (date == null)
            throw new ServiceFailure("0000", "Could not get date information from response's 'Date' header.");

        return date;
    }


    /**
     * Get the resource with the specified pid.  Used by both the CNode and
     * MultipartMNode subclasses. A LocalCache is used to cache objects in memory and in
     * a local disk cache if the "D1Client.useLocalCache" configuration property
     * was set to true when the MultipartD1Node was created. Otherwise
     * InputStream is the Java native version of D1's OctetStream
     *
     * @see <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.get">see DataONE API Reference (MemberNode API)</a>
     * @see <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.get">see DataONE API Reference (CoordinatingNode API)</a>
     */
    public InputStream get(Identifier pid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented, InsufficientResources {
        return get(this.defaultSession, pid);
    }


    /**
     * Get the resource with the specified pid.  Used by both the CNode and
     * MultipartMNode subclasses. A LocalCache is used to cache objects in memory and in
     * a local disk cache if the "D1Client.useLocalCache" configuration property
     * was set to true when the MultipartD1Node was created. Otherwise
     * InputStream is the Java native version of D1's OctetStream
     *
     * @see <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.get">see DataONE API Reference (MemberNode API)</a>
     * @see <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.get">see DataONE API Reference (CoordinatingNode API)</a>
     */
    public InputStream get(Session session, Identifier pid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented, InsufficientResources {
        AutoCloseInputStream is = null;

        // if we have it, stay local
        boolean cacheMissed = false;
        if (useLocalCache) {
            try {
                byte[] data = LocalCache.instance().getData(pid);
                is = new AutoCloseInputStream(new ByteArrayInputStream(data));
                return is;

            } catch (NotCached e) {
                cacheMissed = true;
            }
        }

        if (cacheMissed || !useLocalCache)
        {
            D1Url url = new D1Url(this.getNodeBaseServiceUrl(),Constants.RESOURCE_OBJECTS);
            try {
                url.addNextPathElement(pid.getValue());
            } catch (IllegalArgumentException e) {
                // can't throw InvalidRequest, so use NotFound (technically not invalid, but listObjects call)
                throw new NotFound("0000", "'pid' cannot be null nor empty");
            }
            InputStream remoteStream = null;
            try {
                remoteStream = getRestClient(session).doGetRequest(url.getUrl(),
                        Settings.getConfiguration().getInteger("D1Client.D1Node.get.timeout", null));


                if (cacheMissed) {
                    // only get here if we useLocalCache is true
                    // (we want to add it to the cache)
                    byte[] bytes = null;
                    try {
                        bytes = IOUtils.toByteArray(remoteStream);
                        LocalCache.instance().putData(pid, bytes);
                    } finally {
                        IOUtils.closeQuietly(remoteStream);
                    }
                    is = new AutoCloseInputStream(new ByteArrayInputStream(bytes));
                } else {
                    is = new AutoCloseInputStream(remoteStream);
                }
            } catch (BaseException be) {
                if (be instanceof InvalidToken)      throw (InvalidToken) be;
                if (be instanceof NotAuthorized)     throw (NotAuthorized) be;
                if (be instanceof NotImplemented)    throw (NotImplemented) be;
                if (be instanceof ServiceFailure)    throw (ServiceFailure) be;
                if (be instanceof NotFound)                throw (NotFound) be;
                if (be instanceof InsufficientResources)   throw (InsufficientResources) be;

                throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
            }
            catch (ClientSideException e) {
                throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
            }
            catch (IOException e) {
                throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
            }
        }

        return is;
    }


    public boolean systemMetadataChanged(Session session, Identifier pid, long serialVersion,
            Date dateSystemMetadataLastModified)
                    throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest {
        
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_META_CHANGED);

        SimpleMultipartEntity mpe = new SimpleMultipartEntity();
        if (pid != null)
            mpe.addParamPart("pid", pid.getValue());
        mpe.addParamPart("dateSysMetaLastModified",
                DateTimeMarshaller.serializeDateToUTC(dateSystemMetadataLastModified));
        mpe.addParamPart("serialVersion", String.valueOf(serialVersion));

        InputStream is = null;
        try {
            is = getRestClient(session).doPostRequest(url.getUrl(), mpe, null);

        } catch (BaseException be) {
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        }
        catch (ClientSideException e)  {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); 
        }
        finally {
            MultipartD1Node.closeLoudly(is);
        }
        return true;
    }



    public DescribeResponse describe(Identifier pid)
            throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
            {
        return describe(this.defaultSession, pid);
            }


    public DescribeResponse describe(Session session, Identifier pid)
            throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound {
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);

        // need to check for null or empty values, or else the call transposes
        // into a listObjects
        if(pid == null || pid.getValue().trim().equals(""))
            throw new NotFound("0000", "'pid' cannot be null nor empty");
        url.addNextPathElement(pid.getValue());

        Header[] headers = null;
        Map<String, String> headersMap = new HashMap<String,String>();
        try {
            headers = getRestClient(session).doHeadRequest(url.getUrl(),null);
            for (Header header: headers) {
                if (log.isDebugEnabled())
                    log.debug(String.format("header: %s = %s",
                            header.getName(),
                            header.getValue() ));
                headersMap.put(header.getName(), header.getValue());
            }
        } catch (BaseException be) {
            if (be instanceof InvalidToken)     throw (InvalidToken) be;
            if (be instanceof NotAuthorized)    throw (NotAuthorized) be;
            if (be instanceof NotImplemented)   throw (NotImplemented) be;
            if (be instanceof ServiceFailure)   throw (ServiceFailure) be;
            if (be instanceof NotFound)         throw (NotFound) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        }
        catch (ClientSideException e)            {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        }


        //   	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        String objectFormatIdStr = headersMap.get("DataONE-ObjectFormat");//.get(0);
        String last_modifiedStr = headersMap.get("Last-Modified");//.get(0);
        String content_lengthStr = headersMap.get("Content-Length");//.get(0);
        String checksumStr = headersMap.get("DataONE-Checksum");//.get(0);
        String serialVersionStr = headersMap.get("DataONE-SerialVersion");//.get(0);


        BigInteger content_length;
        try {
            content_length = BigIntegerMarshaller.deserializeBigInteger(content_lengthStr);
        } catch (JiBXException e) {
            throw new ServiceFailure("0", "Could not convert the returned content_length string (" +
                    content_lengthStr + ") to a BigInteger: " + e.getMessage());
        }
        Date last_modified = null;
        try
        {
            if (last_modifiedStr != null)
                last_modified = DateTimeMarshaller.deserializeDateToUTC(last_modifiedStr.trim());
        }
        catch(NullPointerException e)
        {
            throw new ServiceFailure("0", "Could not parse the returned date string " +
                    last_modifiedStr + ". The date string needs to be either ISO" +
                    " 8601 compliant or http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.3.1 compliant: " +
                    e.getMessage());
        }

        // build a checksum object
        Checksum checksum = new Checksum();
        if (checksumStr != null) {
            String[] cs = checksumStr.split(",");
            checksum.setAlgorithm(cs[0]);
            if (cs.length > 1) {
                checksum.setValue(cs[1]);
            } else {
                throw new ServiceFailure("0", "malformed checksum header returned, " +
                        "checksum value not returned in the response");
            }
        }
        // build an objectformat identifier object
        // doesn't check validity of the formatID value

        // to do the check, uncomment following line, and work it in to the code
        //        ObjectFormat format = ObjectFormatCache.getInstance().getFormat(objectFormatIdStr);

        ObjectFormatIdentifier formatId = new ObjectFormatIdentifier();
        formatId.setValue(objectFormatIdStr);

        BigInteger serialVersion = null;
        try {
            serialVersion = BigIntegerMarshaller.deserializeBigInteger(serialVersionStr);
        } catch (JiBXException e) {
            throw new ServiceFailure("0", "Could not convert the returned serialVersion string (" +
                    serialVersionStr + ") to a BigInteger: " + e.getMessage());
        }

        return new DescribeResponse(formatId, content_length, last_modified, checksum, serialVersion);
    }



    public Checksum getChecksum(Identifier pid, String checksumAlgorithm)
            throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound {
        return getChecksum(this.defaultSession, pid, checksumAlgorithm);
    }


    /**
     * This method can handle both the MN and CN method, although the CN overriding method
     * will need to recast the InvalidRequest exception and use 'null' for the checksumAlgorithm param
     * @param defaultSession
     * @param pid
     * @param checksumAlgorithm - for MN implementations only
     * @return
     * @throws InvalidRequest - for MN implementations only
     * @throws InvalidToken
     * @throws NotAuthorized
     * @throws NotImplemented
     * @throws ServiceFailure
     * @throws NotFound
     */
    public Checksum getChecksum(Session session, Identifier pid, String checksumAlgorithm)
            throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound {
        if (pid == null)
            throw new NotFound("0000", "PID cannot be null");

        // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_CHECKSUM);
        url.addNextPathElement(pid.getValue());

        url.addNonEmptyParamPair("checksumAlgorithm", checksumAlgorithm);

        // send the request
        Checksum checksum = null;

        try {
            InputStream is = getRestClient(session).doGetRequest(url.getUrl(),null);
            checksum = deserializeServiceType(Checksum.class, is);
        } catch (BaseException be) {
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof NotFound)               throw (NotFound) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        }
        catch (ClientSideException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        }

        return checksum;
    }



    public boolean isAuthorized(Identifier pid, Permission action)
            throws ServiceFailure, InvalidRequest, InvalidToken, NotFound, NotAuthorized, NotImplemented {
        return isAuthorized(this.defaultSession, pid, action);
    }


    public boolean isAuthorized(Session session, Identifier pid, Permission action)
            throws ServiceFailure, InvalidRequest, InvalidToken, NotFound, NotAuthorized, NotImplemented {
        // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_AUTHORIZATION);
        if (pid != null)
            url.addNextPathElement(pid.getValue());
        if (action != null)
            url.addNonEmptyParamPair("action", action.xmlValue());

        InputStream is = null;
        try {
            is = getRestClient(session).doGetRequest(url.getUrl(),null);

        } catch (BaseException be) {
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof NotFound)               throw (NotFound) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        }
        catch (ClientSideException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); 
        }
        finally {
            MultipartD1Node.closeLoudly(is);
        }
        return true;
    }


    public  Identifier generateIdentifier(String scheme, String fragment)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest {
        return generateIdentifier(this.defaultSession, scheme, fragment);
    }


    public  Identifier generateIdentifier(Session session, String scheme, String fragment)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest {
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_GENERATE);
        SimpleMultipartEntity smpe = new SimpleMultipartEntity();
        if (scheme == null) {
            throw new InvalidRequest("0000","'scheme' cannot be null");
        }
        smpe.addParamPart("scheme", scheme);
        // omit fragment part if null because it is optional for user to include
        // (the service should not rely on the empty parameter to be there)
        if (fragment != null) {
            smpe.addParamPart("fragment", fragment);
        }

        Identifier identifier = null;

        try {
            InputStream is = getRestClient(session).doPostRequest(url.getUrl(),smpe,null);
            identifier = deserializeServiceType(Identifier.class, is);

        } catch (BaseException be) {
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        }
        catch (ClientSideException e)            {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        }

        return identifier;
    }


    public  Identifier archive(Identifier pid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented {
        return archive(this.defaultSession, pid);
    }


    /**
     *  sets the archived flag to true on an MN or CN
     * @param defaultSession
     * @param pid
     * @return Identifier
     * @throws InvalidToken
     * @throws ServiceFailure
     * @throws NotAuthorized
     * @throws NotFound
     * @throws NotImplemented
     * @throws InvalidRequest
     */
    public  Identifier archive(Session session, Identifier pid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented {
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ARCHIVE);
        if (pid != null)
            url.addNextPathElement(pid.getValue());

        Identifier identifier = null;
        try {
            InputStream is = getRestClient(session).doPutRequest(url.getUrl(), null, null);
            identifier = deserializeServiceType(Identifier.class, is);
        } catch (BaseException be) {
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotFound)               throw (NotFound) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        }
        catch (ClientSideException e)            {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

        return identifier;
    }


    public  Identifier delete(Identifier pid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented {
        return delete(this.defaultSession, pid);
    }


    public  Identifier delete(Session session, Identifier pid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented {
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
        if (pid != null)
            url.addNextPathElement(pid.getValue());


        Identifier identifier = null;
        try {
            InputStream is = getRestClient(session).doDeleteRequest(url.getUrl(), null);
            identifier = deserializeServiceType(Identifier.class, is);
        } catch (BaseException be) {
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotFound)               throw (NotFound) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        }
        catch (ClientSideException e)            {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

        return identifier;
    }

    /**
     * As with the sister query() method that takes a D1Url, the contents of
     * encodedQuery will be joined to the url with as follows:
     * /{queryEngine}/{encodedQuery}
     *
     * Note:  Users will need to provide the '?' where appropriate to mark
     * the beginning of the url-query portion.  (for example, solr queries are
     * entirely contained in the query portion of a url, so users need the first
     *  character of the provided encodedQuery to be a '?')
     *
     * @param queryEngine
     * @param encodedQuery
     * @return
     * @throws InvalidToken
     * @throws ServiceFailure
     * @throws NotAuthorized
     * @throws InvalidRequest
     * @throws NotImplemented
     * @throws NotFound
     */
    public InputStream query(Session session, String queryEngine, String encodedQuery)
            throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
            NotImplemented, NotFound {
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_QUERY);
        try {
            url.addNextPathElement(queryEngine);
        } catch (IllegalArgumentException e) {
            throw new InvalidRequest("0000", "'queryEngine' parameter cannot be null or empty");
        }
        if (StringUtils.isEmpty(encodedQuery)) {
            throw new InvalidRequest("0000", "'encodedQuery' parameter cannot be null or empty");
        }
        String finalUrl = url.getUrl() + "/" + encodedQuery;

        AutoCloseInputStream is = null;
        try {
            is = new AutoCloseInputStream(getRestClient(session).doGetRequest(finalUrl, null));
        }
        catch (BaseException be) {
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
            if (be instanceof NotFound)               throw (NotFound) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        }
        catch (ClientSideException e)            {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        }

        return is;
    }



    public QueryEngineDescription getQueryEngineDescription(Session session, String queryEngine)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, NotFound {
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_QUERY);
        try {
            url.addNextPathElement(queryEngine);
        } catch (IllegalArgumentException e) {
            throw new NotFound("0000", "'queryEngine' cannot be null nor empty");
        }

        QueryEngineDescription description = null;
        try {
            InputStream is = getRestClient(session).doGetRequest(url.getUrl(), null);
            description = deserializeServiceType(QueryEngineDescription.class, is);
        }
        catch (BaseException be) {
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof NotFound)               throw (NotFound) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        }
        catch (ClientSideException e)            {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

        return description;
    }



    public QueryEngineList listQueryEngines(Session session) throws InvalidToken, ServiceFailure,
    NotAuthorized, NotImplemented {
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_QUERY);

        InputStream is = null;
        QueryEngineList engines = null;
        try {
            is = getRestClient(session).doGetRequest(url.getUrl(), null);
            engines = deserializeServiceType(QueryEngineList.class, is);
        }
        catch (BaseException be) {
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof InvalidToken)           throw (InvalidToken) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        }
        catch (ClientSideException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

        return engines;
    }


    /**
     * deserialize an object of type from the inputstream
     * This is a wrapper method of the standard common Unmarshalling method
     * that recasts exceptions to ServiceFailure
     *
     * @param type
     *            the class of the object to serialize (i.e.
     *            SystemMetadata.class)
     * @param is
     *            the stream to deserialize from
     * @throws ServiceFailure
     */
    @SuppressWarnings("rawtypes")
    protected <T> T deserializeServiceType(Class<T> domainClass, InputStream is)
            throws ServiceFailure {
        try {
            return TypeMarshaller.unmarshalTypeFromStream(domainClass, is);
        } catch (JiBXException e) {
            throw new ServiceFailure("0",
                    "Could not deserialize the " + domainClass.getCanonicalName() + ": " + e.getMessage());
        } catch (IOException e) {
            throw new ServiceFailure("0",
                    "Could not deserialize the " + domainClass.getCanonicalName() + ": " + e.getMessage());
        } catch (InstantiationException e) {
            throw new ServiceFailure("0",
                    "Could not deserialize the " + domainClass.getCanonicalName() + ": " + e.getMessage());
        } catch (IllegalAccessException e) {
            throw new ServiceFailure("0",
                    "Could not deserialize the " + domainClass.getCanonicalName() + ": " + e.getMessage());
        }
    }
    
    protected InputStream view(Session session, String theme, Identifier id) throws InvalidToken,
            ServiceFailure, NotAuthorized, InvalidRequest, NotImplemented, NotFound {

        D1Url url = new D1Url(this.getNodeBaseServiceUrl(),Constants.RESOURCE_VIEWS);

        if (StringUtils.isBlank(theme))
            url.addNextPathElement("default");
        else
            url.addNextPathElement(theme);

        if (id == null || StringUtils.isBlank(id.getValue()))
            throw new NotFound("0000", "'pid' cannot be null nor empty");
        url.addNextPathElement(id.getValue());
        
        InputStream remoteStream = null;
        try {
            remoteStream = getRestClient(session).doGetRequest(url.getUrl(),
                    Settings.getConfiguration().getInteger("D1Client.D1Node.get.timeout", null));
        } catch (BaseException be) {
            if (be instanceof InvalidToken)      throw (InvalidToken) be;
            if (be instanceof NotAuthorized)     throw (NotAuthorized) be;
            if (be instanceof InvalidRequest)      throw (InvalidRequest) be;
            if (be instanceof NotImplemented)    throw (NotImplemented) be;
            if (be instanceof NotFound)                throw (NotFound) be;
            if (be instanceof ServiceFailure)    throw (ServiceFailure) be;
            
            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        }
        catch (ClientSideException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        }
        
        return new AutoCloseInputStream(remoteStream);
    }

    protected OptionList listViews(Session session) throws InvalidToken, ServiceFailure, NotAuthorized,
            InvalidRequest, NotImplemented {

        if (session == null)
            session = defaultSession;
        
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_VIEWS);
        
        OptionList optionList = null;
        try {
            InputStream is = getRestClient(defaultSession).doGetRequest(url.getUrl(),
                    Settings.getConfiguration().getInteger("D1Client.D1Node.get.timeout", null));
            optionList = deserializeServiceType(OptionList.class, is);
        } catch (BaseException be) {
            if (be instanceof InvalidToken)      throw (InvalidToken) be;
            if (be instanceof NotAuthorized)     throw (NotAuthorized) be;
            if (be instanceof InvalidRequest)      throw (InvalidRequest) be;
            if (be instanceof NotImplemented)    throw (NotImplemented) be;
            if (be instanceof ServiceFailure)    throw (ServiceFailure) be;
            
            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        }
        catch (ClientSideException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        }
        
        return optionList;
    }
}
