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

package org.dataone.client.v2.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Date;

import org.apache.commons.io.input.AutoCloseInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.client.utils.ExceptionUtils;
import org.dataone.client.v2.MNode;
import org.dataone.configuration.Settings;
import org.dataone.mimemultipart.SimpleMultipartEntity;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidSystemMetadata;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.SynchronizationFailed;
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.mn.tier1.v2.MNCore;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.DescribeResponse;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v2.Log;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.OptionList;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.Constants;
import org.dataone.service.util.D1Url;
import org.jibx.runtime.JiBXException;

/**
 * MultipartMNode represents a MemberNode, and exposes the services associated with a
 * DataONE Member Node, allowing calling clients to call the services associated
 * with the node.
 *
 * By necessity, behavior for null values differs slightly from the REST api.
 * Except where noted, null values in method parameters will:
 * 1. throw a NotFound or InvalidRequest exception if the parameter is mapped to the URL path segment
 * 2. omit the url query parameter if the parameter is mapped to a URL query segment
 * 3. throw an InvalidRequest exception if the parameter is mapped to the message body
 *
 * Session objects get passed to the underlying HttpMultipartRestClient class, but  be passed to the httpClient request, as they are
 * pulled from the underlying filesystem of the local machine.
 *
 * Java implementation of the following types:
 * Types.OctectStream - InputStream
 * unsignedlong - we use long to keep to a native type, although only half capacity of unsigned
 * Types.DateTime - java.util.Date
 *
 * Various methods may set their own timeouts by use of Settings.Configuration properties
 * or by calling setDefaultSoTimeout.  Settings.Configuration properties override
 * any value of the DefaultSoTimeout.  Timeouts are always represented in milliseconds
 *
 * timeout properties recognized:
 * D1Client.MNode.create.timeout
 * D1Client.MNode.update.timeout
 * D1Client.MNode.replicate.timeout
 * D1Client.MNode.getReplica.timeout
 * D1Client.D1Node.listObjects.timeout
 * D1Client.D1Node.getLogRecords.timeout
 * D1Client.D1Node.get.timeout
 * D1Client.D1Node.getSystemMetadata.timeout
 *
 *  @author Rob Nahf
 */

public class MultipartMNode extends MultipartD1Node implements MNode
{

    protected static org.apache.commons.logging.Log log = LogFactory.getLog(MultipartMNode.class);

    /**
     * Construct a new client-side MultipartMNode (Member Node) object,
     * passing in the base url of the member node for calling its services.
     * @param nodeBaseServiceUrl base url for constructing service endpoints.
     * @throws ClientSideException
     * @throws IOException
     */
    @Deprecated
    public MultipartMNode(String nodeBaseServiceUrl) throws IOException, ClientSideException {
        super(nodeBaseServiceUrl);
        this.nodeType = NodeType.MN;
    }


    /**
     * Construct a new client-side MultipartMNode (Member Node) object,
     * passing in the base url of the member node for calling its services,
     * and the Session to use for connections to that node.
     * @param nodeBaseServiceUrl base url for constructing service endpoints.
     * @param session - the Session object passed to the CertificateManager
     *                  to be used for establishing connections
     */
//	@Deprecated
//    public MultipartMNode(String nodeBaseServiceUrl, Session session) {
//        super(nodeBaseServiceUrl, session);
//        this.nodeType = NodeType.MN;
//    }

    /**
     * Construct a new client-side MultipartMNode (Member Node) object,
     * passing in the base url of the member node for calling its services.
     * @param nodeBaseServiceUrl base url for constructing service endpoints.
     */
    public MultipartMNode(MultipartRestClient mrc, String nodeBaseServiceUrl) {
        super(mrc, nodeBaseServiceUrl);
        this.nodeType = NodeType.MN;
    }


    /**
     * Construct a new client-side MultipartMNode (Member Node) object,
     * passing in the base url of the member node for calling its services,
     * and the Session to use for connections to that node.
     * @param nodeBaseServiceUrl base url for constructing service endpoints.
     * @param session - the Session object passed to the CertificateManager
     *                  to be used for establishing connections
     */
    public MultipartMNode(MultipartRestClient mrc, String nodeBaseServiceUrl, Session session) {
        super(mrc, nodeBaseServiceUrl, session);
        this.nodeType = NodeType.MN;
    }


    /* (non-Javadoc)
     * @see org.dataone.client.MNode#getNodeBaseServiceUrl()
     */
    @Override
    public String getNodeBaseServiceUrl() {
        D1Url url = new D1Url(super.getNodeBaseServiceUrl());
        url.addNextPathElement(MNCore.SERVICE_VERSION);
        log.debug("Node base service URL is: " + url.getUrl());
        return url.getUrl();
    }


    /* (non-Javadoc)
     * @see org.dataone.client.MNode#ping()
     */
    @Override
    public Date ping() throws NotImplemented, ServiceFailure, InsufficientResources
    {
        return super.ping();
    }

    @Override
    public ObjectList listObjects(Session session, Date fromDate, Date toDate,
      ObjectFormatIdentifier formatid, Identifier identifier, Boolean replicaStatus, Integer start, Integer count)
              throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure
    {

        if (toDate != null && fromDate != null && !toDate.after(fromDate))
            throw new InvalidRequest("1000", "fromDate must be before toDate in listObjects() call. "
                    + fromDate + " " + toDate);

        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);

        url.addDateParamPair("fromDate", fromDate);
        url.addDateParamPair("toDate", toDate);
        if (formatid != null)
            url.addNonEmptyParamPair("formatId", formatid.getValue());
        if (identifier != null)
            url.addNonEmptyParamPair("identifier", identifier.getValue());
        if (replicaStatus != null) {
            if (replicaStatus) {
                url.addNonEmptyParamPair("replicaStatus", 1);
            } else {
                url.addNonEmptyParamPair("replicaStatus", 0);
            }
        }
        url.addNonEmptyParamPair("start",start);
        url.addNonEmptyParamPair("count",count);

        // send the request
        ObjectList objectList = null;
        try {
            InputStream is = getRestClient(session).doGetRequest(url.getUrl(), null);
            objectList =  deserializeServiceType(ObjectList.class, is);
        } catch (BaseException be) {
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        }
        catch (ClientSideException e)            {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        }

        return objectList;
    }


    /* (non-Javadoc)
     * @see org.dataone.client.MNode#getCapabilities()
     */
    @Override
    public Node getCapabilities() throws NotImplemented, ServiceFailure {
        // assemble the url
        D1Url url = new D1Url(getNodeBaseServiceUrl(), Constants.RESOURCE_NODE);

        // send the request
        Node node = null;

        try {
            InputStream is = getRestClient(this.defaultSession).doGetRequest(url.getUrl(), null);
            node = deserializeServiceType(Node.class, is);
        } catch (BaseException be) {
            if (be instanceof NotImplemented)
                throw (NotImplemented) be;
            if (be instanceof ServiceFailure)
                throw (ServiceFailure) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        } catch (ClientSideException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        }

        return node;
    }

    /* (non-Javadoc)
     * @see org.dataone.client.MNode#get(org.dataone.service.types.v1.Identifier)
     */
    @Override
    public InputStream get(Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound, InsufficientResources
    {
        return super.get(pid);
    }


    /* (non-Javadoc)
     * @see org.dataone.client.MNode#get(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier)
     */
    @Override
    public InputStream get(Session session, Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound, InsufficientResources
    {
        return super.get(session, pid);
    }

    public Log getLogRecords(Session session, Date fromDate, Date toDate,
            String event, String idFilter, Integer start, Integer count)
    throws InvalidToken, InvalidRequest, ServiceFailure,
    NotAuthorized, NotImplemented
    {

        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_LOG);

        url.addDateParamPair("fromDate", fromDate);
        url.addDateParamPair("toDate", toDate);

        if (event != null)
            url.addNonEmptyParamPair("event", event);

        url.addNonEmptyParamPair("start", start);
        url.addNonEmptyParamPair("count", count);
        url.addNonEmptyParamPair("idFilter", idFilter);

        // send the request
        Log log = null;

        try {
            InputStream is = getRestClient(session).doGetRequest(url.getUrl(),
                    Settings.getConfiguration().getInteger("D1Client.D1Node.getLogRecords.timeout", null));
            log = deserializeServiceType(Log.class, is);
        } catch (BaseException be) {
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        }
        catch (ClientSideException e)            {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        }

        return log;
    }


    /**
     * Get the system metadata from a resource with the specified guid, potentially using the local
     * system metadata cache if specified to do so. Used by both the CNode and MultipartMNode implementations.
     * Because SystemMetadata is mutable, caching can lead to currency issues.  In specific
     * cases where a client wants to utilize the same system metadata in rapid succession,
     * it may make sense to temporarily use the local cache by setting useSystemMetadadataCache to true.
     * @see https://purl.dataone.org/architecturev2/apis/MN_APIs.html#MNRead.getSystemMetadata"> DataONE API Reference (MemberNode API)</a>
     * @see https://purl.dataone.org/architecturev2/apis/CN_APIs.html#CNRead.getSystemMetadata"> DataONE API Reference (CoordinatingNode API)</a>
     */
    @Override
    public SystemMetadata getSystemMetadata(Session session, Identifier id)
    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
    {
        return super.getSystemMetadata(session, id);
    }




    /* (non-Javadoc)
     * @see org.dataone.client.MNode#describe(org.dataone.service.types.v1.Identifier)
     */
    @Override
    public DescribeResponse describe(Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
        return super.describe(pid);
    }

    /* (non-Javadoc)
     * @see org.dataone.client.MNode#describe(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier)
     */
    @Override
    public DescribeResponse describe(Session session, Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
        return super.describe(session,pid);
    }


    /* (non-Javadoc)
     * @see org.dataone.client.MNode#getChecksum(org.dataone.service.types.v1.Identifier, String)
     */
    @Override
    public Checksum getChecksum(Identifier pid, String checksumAlgorithm)
    throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
        return super.getChecksum(pid, checksumAlgorithm);
    }


    /* (non-Javadoc)
     * @see org.dataone.client.MNode#getChecksum(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, String)
     */
    @Override
    public Checksum getChecksum(Session session, Identifier pid, String checksumAlgorithm)
    throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
        return super.getChecksum(session, pid, checksumAlgorithm);
    }


    /* (non-Javadoc)
     * @see org.dataone.client.MNode#synchronizationFailed(org.dataone.service.types.v1.Session, org.dataone.service.exceptions.SynchronizationFailed)
     */
    @Override
    public boolean synchronizationFailed(Session session, SynchronizationFailed message)
            throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure
    {
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ERROR);
        SimpleMultipartEntity mpe = new SimpleMultipartEntity();
        try {
            mpe.addFilePart("message", message.serialize(SynchronizationFailed.FMT_XML));
        } catch (IOException e1) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
        }

        // send the request


        InputStream is = null;
        try {
            is = getRestClient(session).doPostRequest(url.getUrl(),mpe, null);

        } catch (BaseException be) {
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;

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


    /* (non-Javadoc)
     * @see org.dataone.client.MNode#isAuthorized(org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.Permission)
     */
    @Override
    public boolean isAuthorized(Identifier pid, Permission action)
    throws ServiceFailure, InvalidRequest, InvalidToken, NotFound, NotAuthorized, NotImplemented
    {
        return super.isAuthorized(pid, action);
    }


    /* (non-Javadoc)
     * @see org.dataone.client.MNode#isAuthorized(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.Permission)
     */
    @Override
    public boolean isAuthorized(Session session, Identifier pid, Permission action)
    throws ServiceFailure, InvalidRequest, InvalidToken, NotFound, NotAuthorized, NotImplemented
    {
        return super.isAuthorized(session, pid, action);
    }


    /* (non-Javadoc)
     * @see org.dataone.client.MNode#generateIdentifier(String, String)
     */
    @Override
    public  Identifier generateIdentifier(String scheme, String fragment)
    throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest
    {
        return super.generateIdentifier(scheme, fragment);
    }

    /* (non-Javadoc)
     * @see org.dataone.client.MNode#generateIdentifier(org.dataone.service.types.v1.Session, String, String)
     */
    @Override
    public  Identifier generateIdentifier(Session session, String scheme, String fragment)
    throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest
    {
        return super.generateIdentifier(session, scheme, fragment);
    }



    /* (non-Javadoc)
     * @see org.dataone.client.MNode#create(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, InputStream, org.dataone.service.types.v1.SystemMetadata)
     */
    @Override
    public  Identifier create(Session session, Identifier pid, InputStream object,
            SystemMetadata sysmeta)
    throws IdentifierNotUnique, InsufficientResources, InvalidRequest, InvalidSystemMetadata,
            InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, UnsupportedType
    {
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);

        SimpleMultipartEntity mpe = new SimpleMultipartEntity();
        try {
            mpe.addParamPart("pid", pid.getValue());
            mpe.addFilePart("object",object);
            mpe.addFilePart("sysmeta", sysmeta);
        } catch (IOException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        } catch (JiBXException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        }

        Identifier identifier = null;

        try {
            InputStream is = getRestClient(session).doPostRequest(url.getUrl(),mpe,
                    Settings.getConfiguration().getInteger("D1Client.MNode.create.timeout", null));
             identifier = deserializeServiceType(Identifier.class, is);
        } catch (BaseException be) {
            if (be instanceof IdentifierNotUnique)    throw (IdentifierNotUnique) be;
            if (be instanceof InsufficientResources)  throw (InsufficientResources) be;
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
            if (be instanceof InvalidSystemMetadata)  throw (InvalidSystemMetadata) be;
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof UnsupportedType)        throw (UnsupportedType) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        }
        catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

        return identifier;
    }

    @Override
    public boolean updateSystemMetadata(Session session, Identifier pid, SystemMetadata sysmeta)
        throws NotImplemented, NotAuthorized,ServiceFailure, InvalidRequest,
        InvalidSystemMetadata, InvalidToken {

        return super.updateSystemMetadata(session, pid, sysmeta);
    }


    /* (non-Javadoc)
     * @see org.dataone.client.MNode#update(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, InputStream, org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.SystemMetadata)
     */
    @Override
    public  Identifier update(Session session, Identifier pid, InputStream object,
            Identifier newPid, SystemMetadata sysmeta)
        throws IdentifierNotUnique, InsufficientResources, InvalidRequest, InvalidSystemMetadata,
            InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, UnsupportedType,
            NotFound
    {
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
        url.addNextPathElement(pid.getValue());

        SimpleMultipartEntity mpe = new SimpleMultipartEntity();
//    	mpe.addParamPart("newPid", EncodingUtilities.encodeUrlQuerySegment(newPid.getValue()));
        mpe.addParamPart("newPid", newPid.getValue());
        try {
            mpe.addFilePart("object",object);
            mpe.addFilePart("sysmeta", sysmeta);
        } catch (IOException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        } catch (JiBXException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        }

        Identifier identifier = null;

        try {
            InputStream is = getRestClient(session).doPutRequest(url.getUrl(),mpe,
                    Settings.getConfiguration()
                    .getInteger("D1Client.MNode.update.timeout",null));

            identifier = deserializeServiceType(Identifier.class, is);
        } catch (BaseException be) {
            if (be instanceof IdentifierNotUnique)    throw (IdentifierNotUnique) be;
            if (be instanceof InsufficientResources)  throw (InsufficientResources) be;
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
            if (be instanceof InvalidSystemMetadata)  throw (InvalidSystemMetadata) be;
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof UnsupportedType)        throw (UnsupportedType) be;
            if (be instanceof NotFound)               throw (NotFound) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        }
        catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

        return identifier;
    }


    /* (non-Javadoc)
     * @see org.dataone.client.MNode#archive(org.dataone.service.types.v1.Identifier)
     */
    @Override
    public  Identifier archive(Identifier pid)
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
    {
        return  super.archive(pid);
    }


    /* (non-Javadoc)
     * @see org.dataone.client.MNode#archive(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier)
     */
    @Override
    public  Identifier archive(Session session, Identifier pid)
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
    {
        return super.archive(session, pid);
    }


    /* (non-Javadoc)
     * @see org.dataone.client.MNode#delete(org.dataone.service.types.v1.Identifier)
     */
    @Override
    public  Identifier delete(Identifier pid)
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
    {
        return  super.delete(pid);
    }


    /* (non-Javadoc)
     * @see org.dataone.client.MNode#delete(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier)
     */
    @Override
    public  Identifier delete(Session session, Identifier pid)
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
    {
         return super.delete(session, pid);
    }


    /* (non-Javadoc)
     * @see org.dataone.client.MNode#replicate(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.SystemMetadata, org.dataone.service.types.v1.NodeReference)
     */
    @Override
    public boolean replicate(Session session, SystemMetadata sysmeta, NodeReference sourceNode)
    throws NotImplemented, ServiceFailure, NotAuthorized, InvalidRequest, InvalidToken,
        InsufficientResources, UnsupportedType
    {
        // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_REPLICATE);

        // assemble the context body
        SimpleMultipartEntity smpe = new SimpleMultipartEntity();
        if (sourceNode != null)
            smpe.addParamPart("sourceNode", sourceNode.getValue());

        try {
            smpe.addFilePart("sysmeta", sysmeta);
        } catch (IOException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        } catch (JiBXException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        }

        InputStream is = null;
        try {
            is = getRestClient(session).doPostRequest(url.getUrl(),smpe,
                    Settings.getConfiguration() .getInteger("D1Client.MNode.replicate.timeout",null));

        } catch (BaseException be) {
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
            if (be instanceof InvalidToken)	          throw (InvalidToken) be;
            if (be instanceof InsufficientResources)  throw (InsufficientResources) be;
            if (be instanceof UnsupportedType)        throw (UnsupportedType) be;

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




    /* (non-Javadoc)
     * @see org.dataone.client.MNode#getReplica(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier)
     */
    @Override
    public InputStream getReplica(Session session, Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound,
    InsufficientResources
    {
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(),Constants.RESOURCE_REPLICAS);
        if (pid != null)
            url.addNextPathElement(pid.getValue());

        InputStream is = null;
        try {
            is = new AutoCloseInputStream(getRestClient(session).doGetRequest(url.getUrl(),
                    Settings.getConfiguration().getInteger("D1Client.MNode.getReplica.timeout", null)));

        } catch (BaseException be) {
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof NotFound)               throw (NotFound) be;
            if (be instanceof InsufficientResources)  throw (InsufficientResources) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        }
        catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

        return is;
    }

    @Override
    public InputStream view(Session session, String theme, Identifier id) throws InvalidToken,
    ServiceFailure, NotAuthorized, InvalidRequest, NotImplemented, NotFound {
        return super.view(session, theme, id);
    }


    @Override
    public OptionList listViews(Session session) throws InvalidToken, ServiceFailure, NotAuthorized,
    InvalidRequest, NotImplemented {
        return super.listViews(session);
    }


    @Override
    public InputStream getPackage(Session session, ObjectFormatIdentifier packageType, Identifier id)
            throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest, NotImplemented,
            NotFound, UnsupportedType {

        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_PACKAGES);

        if (packageType == null || StringUtils.isBlank(packageType.getValue()))
            url.addNextPathElement("application/zip");
        else
            url.addNextPathElement(packageType.getValue());
        
        if (StringUtils.isBlank(id.getValue()))
            throw new NotFound("0000", "'pid' cannot be null nor empty");
        
        url.addNextPathElement(id.getValue());
        
        InputStream remoteStream = null;
        try {
            remoteStream = getRestClient(session).doGetRequest(url.getUrl(),120000);
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
}
