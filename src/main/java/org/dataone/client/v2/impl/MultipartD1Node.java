package org.dataone.client.v2.impl;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.client.utils.ExceptionUtils;
import org.dataone.configuration.Settings;
import org.dataone.mimemultipart.SimpleMultipartEntity;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidSystemMetadata;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.Constants;
import org.dataone.service.util.D1Url;
import org.jibx.runtime.JiBXException;

/**
 * A v2-specific abstract subclass for new (non-v1) common methods.
 * @author rnahf
 *
 */
public abstract class MultipartD1Node extends org.dataone.client.rest.MultipartD1Node {

    public MultipartD1Node(MultipartRestClient client, String nodeBaseServiceUrl, Session session) {
        super(client, nodeBaseServiceUrl, session);
    }

    public MultipartD1Node(MultipartRestClient client, String nodeBaseServiceUrl) {
        super(client, nodeBaseServiceUrl, /* Session */ null);
    }

    @Deprecated
    public MultipartD1Node(String nodeBaseServiceUrl) throws IOException, ClientSideException {
        super(nodeBaseServiceUrl);
    }



    public Node getCapabilities()
    throws NotImplemented, ServiceFailure
    {
        // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_NODE);

        // send the request
        Node node = null;

        try {
            InputStream is = getRestClient(this.defaultSession).doGetRequest(url.getUrl(),null);
            node = deserializeServiceType(Node.class, is);
        } catch (BaseException be) {
            if (be instanceof NotImplemented)    throw (NotImplemented) be;
            if (be instanceof ServiceFailure)    throw (ServiceFailure) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        }
        catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

        return node;
    }

    /**
     * Get the system metadata from a resource with the specified guid, potentially using the local
     * system metadata cache if specified to do so. Used by both the CNode and MultipartMNode implementations.
     * Because SystemMetadata is mutable, caching can lead to currency issues.  In specific
     * cases where a client wants to utilize the same system metadata in rapid succession,
     * it may make sense to temporarily use the local cache by setting useSystemMetadadataCache to true.
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.getSystemMetadata"> DataONE API Reference (MemberNode API)</a>
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.getSystemMetadata"> DataONE API Reference (CoordinatingNode API)</a>
     */
    public SystemMetadata getSystemMetadata(Session session, Identifier pid)
    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
    {

        D1Url url = new D1Url(this.getNodeBaseServiceUrl(),Constants.RESOURCE_META);
        if (pid != null)
            url.addNextPathElement(pid.getValue());


        InputStream is = null;
        SystemMetadata sysmeta = null;

        try {
            is = getRestClient(session).doGetRequest(url.getUrl(),
                    Settings.getConfiguration().getInteger("D1Client.D1Node.getSystemMetadata.timeout", null));
            sysmeta = deserializeServiceType(SystemMetadata.class,is);

        } catch (BaseException be) {
            if (be instanceof InvalidToken)      throw (InvalidToken) be;
            if (be instanceof NotAuthorized)     throw (NotAuthorized) be;
            if (be instanceof NotImplemented)    throw (NotImplemented) be;
            if (be instanceof ServiceFailure)    throw (ServiceFailure) be;
            if (be instanceof NotFound)          throw (NotFound) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        }
        catch (ClientSideException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        }
        return sysmeta;
    }



    public boolean updateSystemMetadata(Session session, Identifier pid,SystemMetadata sysmeta)
            throws NotImplemented, NotAuthorized,ServiceFailure, InvalidRequest,
            InvalidSystemMetadata, InvalidToken {
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_META);
        if (pid == null) {
            throw new InvalidRequest("0000","'pid' cannot be null");
        }

        SimpleMultipartEntity mpe = new SimpleMultipartEntity();
        try {
            mpe.addParamPart("pid", pid.getValue());
            mpe.addFilePart("sysmeta", sysmeta);
        } catch (IOException e1) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
        } catch (JiBXException e1) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
        }

        InputStream is = null;
        try {
            is = getRestClient(session).doPutRequest(url.getUrl(),mpe,
                    Settings.getConfiguration().getInteger("D1Client.CNode.registerSystemMetadata.timeouts", null));
        } catch (BaseException be) {
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
            if (be instanceof InvalidSystemMetadata)  throw (InvalidSystemMetadata) be;
            if (be instanceof InvalidToken)           throw (InvalidToken) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        } catch (ClientSideException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        } finally {
            IOUtils.closeQuietly(is);
        }

        return true;
    }
}
