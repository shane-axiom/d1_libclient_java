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
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.rest.MultipartD1Node;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.client.utils.ExceptionUtils;
import org.dataone.client.v2.CNode;
import org.dataone.client.v2.formats.ObjectFormatCache;
import org.dataone.configuration.Settings;
import org.dataone.mimemultipart.SimpleMultipartEntity;
import org.dataone.service.cn.v2.CNCore;
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
import org.dataone.service.exceptions.VersionMismatch;
import org.dataone.service.types.v1.AccessPolicy;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.ChecksumAlgorithmList;
import org.dataone.service.types.v1.Group;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.ObjectLocationList;
import org.dataone.service.types.v1.Person;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationPolicy;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SubjectInfo;
import org.dataone.service.types.v2.Log;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.NodeList;
import org.dataone.service.types.v2.ObjectFormat;
import org.dataone.service.types.v2.ObjectFormatList;
import org.dataone.service.types.v2.OptionList;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.Constants;
import org.dataone.service.util.D1Url;
import org.dataone.service.util.TypeMarshaller;
import org.jibx.runtime.JiBXException;

/**
 * CNode represents a DataONE Coordinating Node, and allows calling classes to
 * execute CN services.
 * 
 * The additional methods lookupNodeBaseUrl(..) and lookupNodeId(..) cache nodelist 
 * information for performing baseUrl lookups and the reverse. The cache expiration
 * is controlled by the property "CNode.nodemap.cache.refresh.interval.seconds" and
 * is configured to 1 hour
 * 
 * 
 * Various methods may set their own timeouts by use of Settings.Configuration properties
 * or by calling setDefaultSoTimeout.  Settings.Configuration properties override
 * any value of the DefaultSoTimeout.  Timeouts are always represented in milliseconds
 * 
 * timeout properties recognized:
 * D1Client.CNode.reserveIdentifier.timeout
 * D1Client.CNode.create.timeout
 * D1Client.CNode.registerSystemMetadata.timeout
 * D1Client.CNode.search.timeout
 * D1Client.CNode.replication.timeout
 * D1Client.D1Node.listObjects.timeout
 * D1Client.D1Node.getLogRecords.timeout
 * D1Client.D1Node.get.timeout
 * D1Client.D1Node.getSystemMetadata.timeout
 * 
 */
public class MultipartCNode extends MultipartD1Node implements CNode 
{
	protected static org.apache.commons.logging.Log log = LogFactory.getLog(MultipartCNode.class);

    private Integer nodelistRefreshIntervalSeconds = 2 * 60;
	
 //   private static final String REPLICATION_TIMEOUT_PROPERTY = "D1Client.CNode.replication.timeout";
	
	/**
	 * Construct a Coordinating Node, passing in the base url for node services. The CN
	 * first retrieves a list of other nodes that can be used to look up node
	 * identifiers and base urls for further service invocations.
	 *
	 * @param nodeBaseServiceUrl base url for constructing service endpoints.
	 * @throws ClientSideException 
	 * @throws IOException 
	 */
    @Deprecated
	public MultipartCNode(String nodeBaseServiceUrl) throws IOException, ClientSideException {
		super(nodeBaseServiceUrl);
        this.nodeType = NodeType.CN;
		nodelistRefreshIntervalSeconds = Settings.getConfiguration()
			.getInteger("CNode.nodemap.cache.refresh.interval.seconds", 
						nodelistRefreshIntervalSeconds);
	}

	
	/**
	 * Construct a Coordinating Node, passing in the base url for node services, 
	 * and the Session to use for connections to that node.  The CN
	 * first retrieves a list of other nodes that can be used to look up node
	 * identifiers and base urls for further service invocations.
	 *
	 * @param nodeBaseServiceUrl base url for constructing service endpoints.
	 * @param defaultSession - the Session object passed to the CertificateManager
     *                  to be used for establishing connections
	 */
//	@Deprecated
//	public MultipartCNode(String nodeBaseServiceUrl, Session session) {
//		super(nodeBaseServiceUrl, session);
//        this.nodeType = NodeType.CN;
//		nodelistRefreshIntervalSeconds = Settings.getConfiguration()
//			.getInteger("CNode.nodemap.cache.refresh.interval.seconds", 
//						nodelistRefreshIntervalSeconds);
//	}

    /**
     * Construct a new client-side MultipartCNode (Coordinating Node) object, 
     * passing in the base url of the member node for calling its services.
     * @param nodeBaseServiceUrl base url for constructing service endpoints.
     */
    public MultipartCNode(MultipartRestClient mrc, String nodeBaseServiceUrl) {
        super(mrc, nodeBaseServiceUrl);
        this.nodeType = NodeType.CN;
        nodelistRefreshIntervalSeconds = Settings.getConfiguration()
                .getInteger("CNode.nodemap.cache.refresh.interval.seconds", 
                            nodelistRefreshIntervalSeconds);
    }
   
    
    /**
     * Construct a new client-side MultipartCNode (Coordinating Node) object, 
     * passing in the base url of the member node for calling its services,
     * and the Session to use for connections to that node. 
     * @param nodeBaseServiceUrl base url for constructing service endpoints.
     * @param defaultSession - the Session object passed to the CertificateManager
     *                  to be used for establishing connections
     */
    public MultipartCNode(MultipartRestClient mrc, String nodeBaseServiceUrl, Session session) {
        super(mrc, nodeBaseServiceUrl, session);
        this.nodeType = NodeType.CN;
        nodelistRefreshIntervalSeconds = Settings.getConfiguration()
                .getInteger("CNode.nodemap.cache.refresh.interval.seconds", 
                            nodelistRefreshIntervalSeconds);
    }	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#getNodeBaseServiceUrl()
	 */
	@Override
	public String getNodeBaseServiceUrl() {
		D1Url url = new D1Url(super.getNodeBaseServiceUrl());
		url.addNextPathElement(CNCore.SERVICE_VERSION);
		return url.getUrl();
	}
    
    
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#listFormats()
	 */
	@Override
	public  ObjectFormatList listFormats()
	throws ServiceFailure, NotImplemented
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_FORMATS);

		// send the request                
		ObjectFormatList formatList = null;
		
		try {			
			InputStream is = getRestClient(this.defaultSession).doGetRequest(url.getUrl(), null);
			formatList = deserializeServiceType(ObjectFormatList.class, is);

		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

		return formatList;
	}


    @Override
    public ObjectFormatIdentifier addFormat(Session session, ObjectFormatIdentifier formatid, ObjectFormat format)
            throws ServiceFailure, NotFound, NotImplemented, InvalidRequest, NotAuthorized, InvalidToken {

        if (formatid == null || StringUtils.isBlank(formatid.getValue()))
            throw new NotFound("0000", "'formatid' cannot be null nor empty");
        if (format == null)
            throw new ServiceFailure("0000", "'format' cannot be null");

        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_FORMATS);
        url.addNextPathElement(formatid.getValue());
        
        ObjectFormatIdentifier formatID = null;
        try {
            SimpleMultipartEntity mpe = new SimpleMultipartEntity();
            mpe.addFilePart("format", format);
            
            InputStream is = getRestClient(this.defaultSession).doPostRequest(url.getUrl(), mpe, null);
            formatID = deserializeServiceType(ObjectFormatIdentifier.class, is);

        } catch (BaseException be) {
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof NotFound)               throw (NotFound) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
            
        } catch (JiBXException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);  
        } catch (IOException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        } catch (ClientSideException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        }
        
        return formatID;
    }
	
	/**
	 *  Return the ObjectFormat for the given ObjectFormatIdentifier, obtained 
	 *  either from a client-cached ObjectFormatList from the ObjectFormatCache,
	 *  or from a call to the CN.
	 *  Caching is enabled by default in production via the property 
	 *  "CNode.useObjectFormatCache" accessed via org.dataone.configuration.Settings
	 *  and the cn baseURL found in D1Client.CN_URL is used for the connection, 
	 *  not the one held by this instance of CNode.
	 *  
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.getFormat">see DataONE API Reference</a> } 
	 */
	@Override
	public  ObjectFormat getFormat(ObjectFormatIdentifier formatid)
	throws ServiceFailure, NotFound, NotImplemented
	{
		ObjectFormat objectFormat = null;
		boolean useObjectFormatCache = false;

		useObjectFormatCache = 
			Settings.getConfiguration().getBoolean("CNode.useObjectFormatCache", useObjectFormatCache);

		if ( useObjectFormatCache ) {
			try {
				objectFormat = ObjectFormatCache.getInstance().getFormat(formatid);

			} catch (BaseException be) {
				if (be instanceof ServiceFailure)        throw (ServiceFailure) be;
				if (be instanceof NotFound)              throw (NotFound) be;
				if (be instanceof NotImplemented)        throw (NotImplemented) be;

				throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
			} 

		} else {
			D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_FORMATS);    
			url.addNextPathElement(formatid.getValue());

			// send the request

			try {
				InputStream is = getRestClient(this.defaultSession).doGetRequest(url.getUrl(), null);
				objectFormat = deserializeServiceType(ObjectFormat.class, is);

			} catch (BaseException be) {
				if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
				if (be instanceof NotFound)               throw (NotFound) be;
				if (be instanceof NotImplemented)         throw (NotImplemented) be;

				throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
			} 
			catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

		}
		return objectFormat;

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
	 * @see org.dataone.client.CNode#listChecksumAlgorithms()
	 */
	@Override
	public ChecksumAlgorithmList listChecksumAlgorithms() throws ServiceFailure, NotImplemented 
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_CHECKSUM);

		ChecksumAlgorithmList algorithmList = null;
		try {
			InputStream is = getRestClient(this.defaultSession).doGetRequest(url.getUrl(), null);
			algorithmList = deserializeServiceType(ChecksumAlgorithmList.class, is);
		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

		return algorithmList;
	}


	public Log getLogRecords(Session session, Date fromDate, Date toDate,
			String event, String pidFilter, Integer start, Integer count) 
	throws InvalidToken, InvalidRequest, ServiceFailure,
	NotAuthorized, NotImplemented, InsufficientResources
	{

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_LOG);

		url.addDateParamPair("fromDate", fromDate);
		url.addDateParamPair("toDate", toDate);
            
    	if (event != null)
            url.addNonEmptyParamPair("event", event);
    	
    	url.addNonEmptyParamPair("start", start);  
    	url.addNonEmptyParamPair("count", count);
    	url.addNonEmptyParamPair("pidFilter", pidFilter);
    	
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
			if (be instanceof InsufficientResources)  throw (InsufficientResources) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)            {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); 
		} 

		return log;
	}
	
	@Override
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

	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#listNodes()
	 */
    @Override
	public NodeList listNodes() throws NotImplemented, ServiceFailure
    {
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_NODE);

		// send the request
		NodeList nodelist = null;

		try {
			InputStream is = getRestClient(this.defaultSession).doGetRequest(url.getUrl(), null);
			nodelist = deserializeServiceType(NodeList.class, is);
			
		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

		return nodelist;
	}
    
    /* (non-Javadoc)
	 * @see org.dataone.client.CNode#listNodes()
	 */
    @Override
	public Node getNodeCapabilities(NodeReference nodeRef) throws NotImplemented, ServiceFailure
    {
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_NODE);
        url.addNextPathElement(nodeRef.getValue());
        
		// send the request
		Node node = null;

		try {
			InputStream is = getRestClient(this.defaultSession).doGetRequest(url.getUrl(), null);
			node = deserializeServiceType(Node.class, is);
			
		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

		return node;
	}

    
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#reserveIdentifier(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier)
	 */
	@Override
	public Identifier reserveIdentifier(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, 
	NotImplemented, InvalidRequest
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_RESERVE);
		SimpleMultipartEntity smpe = new SimpleMultipartEntity();

		if (pid != null) {
			smpe.addParamPart("pid", pid.getValue());
		} else {
			throw new InvalidRequest("0000","PID cannot be null");
		}
		
		Identifier identifier = null;
 		try {
 			InputStream is = getRestClient(session).doPostRequest(url.getUrl(),smpe,
 					Settings.getConfiguration().getInteger("D1Client.CNode.reserveIdentifier.timeout", null));
 			identifier = deserializeServiceType(Identifier.class, is);
 			
		} catch (BaseException be) {
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof IdentifierNotUnique)    throw (IdentifierNotUnique) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

 		return identifier;
	}
	

	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#hasReservation(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Subject, org.dataone.service.types.v1.Identifier)
	 */
	@Override
	public boolean hasReservation(Session session, Subject subject, Identifier pid)
	throws InvalidToken, ServiceFailure,  NotFound, NotAuthorized, 
	NotImplemented, IdentifierNotUnique
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_RESERVE);
		
    	try {
    		if (subject != null)
    			url.addNonEmptyParamPair("subject", subject.getValue());
			if (pid != null)
				url.addNextPathElement(pid.getValue());
		} catch (Exception e) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
		}
    	
		// send the request
		
		try {
			InputStream is = getRestClient(session).doGetRequest(url.getUrl(), null);
			if (is != null)
				is.close();
			
		} catch (BaseException be) {
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof IdentifierNotUnique)    throw (IdentifierNotUnique) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); 
		} 
		catch (IOException e) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
		}

		return true;
	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#create(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, .InputStream, org.dataone.service.types.v1.SystemMetadata)
	 */
	@Override
	public Identifier create(Session session, Identifier pid, InputStream object,
			SystemMetadata sysmeta) 
	throws InvalidToken, ServiceFailure,NotAuthorized, IdentifierNotUnique, UnsupportedType,
	InsufficientResources, InvalidSystemMetadata, NotImplemented, InvalidRequest
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
		if (pid == null) {
			throw new InvalidRequest("0000", "PID cannot be null");
		}

        SimpleMultipartEntity mpe = new SimpleMultipartEntity();

        // Coordinating Nodes must maintain systemmetadata of all object on dataone
        // however Coordinating nodes do not house Science Data only Science Metadata
        // Thus, the inputstream for an object may be null
        // so deal with it here ...
        // and this is how CNs are different from MNs
        // because if object is null on an MN, we should throw an exception

        try {
        	// pid as param, not in URL
        	mpe.addParamPart("pid", pid.getValue());
        	if (object == null) {
        		// object sent is an empty string
        		mpe.addFilePart("object", "");
        	} else {
        		mpe.addFilePart("object", object);
        	}
        	mpe.addFilePart("sysmeta", sysmeta);
        } catch (IOException e) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
		} catch (JiBXException e) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);	
		}

        Identifier identifier = null;

        try {
        	InputStream is = getRestClient(session).doPostRequest(url.getUrl(), mpe,
        			Settings.getConfiguration().getInteger("D1Client.CNode.create.timeouts", null));
        	identifier = deserializeServiceType(Identifier.class, is);
        	
		} catch (BaseException be) {
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof IdentifierNotUnique)    throw (IdentifierNotUnique) be;
			if (be instanceof UnsupportedType)        throw (UnsupportedType) be;
			if (be instanceof InsufficientResources)  throw (InsufficientResources) be;
			if (be instanceof InvalidSystemMetadata)  throw (InvalidSystemMetadata) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

 		return identifier;
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#registerSystemMetadata(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.SystemMetadata)
	 */
	@Override
	public Identifier registerSystemMetadata(Session session, Identifier pid,
		SystemMetadata sysmeta) 
	throws NotImplemented, NotAuthorized,ServiceFailure, InvalidRequest, 
	InvalidSystemMetadata, InvalidToken
	{
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

		Identifier identifier = null;
		try {
			InputStream is = getRestClient(session).doPostRequest(url.getUrl(),mpe,
					Settings.getConfiguration().getInteger("D1Client.CNode.registerSystemMetadata.timeouts", null));
			identifier = deserializeServiceType(Identifier.class, is);
		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof InvalidSystemMetadata)  throw (InvalidSystemMetadata) be;
			if (be instanceof InvalidToken)	          throw (InvalidToken) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

 		return identifier;
	}
	
	@Override
    public boolean synchronize(Session session, Identifier pid)
            throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest,
            InvalidSystemMetadata, InvalidToken {

	    if (pid == null)
            throw new InvalidRequest("0000", "'pid' cannot be null");
	    
	    D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_SYNCHRONIZE);

        SimpleMultipartEntity mpe = new SimpleMultipartEntity();
        mpe.addParamPart("pid", pid.getValue());

        try {
            InputStream is = getRestClient(session).doPostRequest(
                    url.getUrl(),
                    mpe,
                    Settings.getConfiguration().getInteger(
                            "D1Client.CNode.registerSystemMetadata.timeouts", null));
        } catch (BaseException be) {
            if (be instanceof NotImplemented)
                throw (NotImplemented) be;
            if (be instanceof NotAuthorized)
                throw (NotAuthorized) be;
            if (be instanceof ServiceFailure)
                throw (ServiceFailure) be;
            if (be instanceof InvalidRequest)
                throw (InvalidRequest) be;
            if (be instanceof InvalidSystemMetadata)
                throw (InvalidSystemMetadata) be;
            if (be instanceof InvalidToken)
                throw (InvalidToken) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        } catch (ClientSideException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        }

        return true;
    }
	
	@Override
    public boolean updateSystemMetadata(Session session, Identifier pid, SystemMetadata sysmeta)
            throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest,
            InvalidSystemMetadata, InvalidToken {

        throw new NotImplemented("0000", "Not yet implemented.");
    }
	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#setObsoletedBy(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.Identifier, long)
	 */
	@Override
	public boolean setObsoletedBy(Session session, Identifier pid,
			Identifier obsoletedByPid, long serialVersion)
			throws NotImplemented, NotFound, NotAuthorized, ServiceFailure,
			InvalidRequest, InvalidToken, VersionMismatch {
		
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_META_OBSOLETEDBY);
		
		if (pid == null) {
			throw new InvalidRequest("0000","'pid' cannot be null");
		}
		
		url.addNextPathElement(pid.getValue());
		
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	if (obsoletedByPid != null)
    		mpe.addParamPart("obsoletedByPid", obsoletedByPid.getValue());
		mpe.addParamPart("serialVersion", String.valueOf(serialVersion));

		try {
			InputStream is = getRestClient(session).doPutRequest(url.getUrl(), mpe, null);
			if (is != null)
				is.close();

		} catch (BaseException be) {
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof VersionMismatch)        throw (VersionMismatch) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); 
		} 
		catch (IOException e) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); 
		} 

		return true;
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
	
	public SystemMetadata getSystemMetadata(Identifier pid)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented 
	{
		return this.getSystemMetadata(null, pid);
	}


    
    /* (non-Javadoc)
	 * @see org.dataone.client.CNode#resolve(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier)
	 */
	@Override
	public ObjectLocationList resolve(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_RESOLVE);
		if (pid == null) {
			throw new NotFound("0000", "'pid' cannot be null");
		}
        url.addNextPathElement(pid.getValue());
		
		ObjectLocationList oll = null;

		try {
            // we set followRedirect to false so we get the ObjectLocationList back
            InputStream is = getRestClient(session).doGetRequest(url.getUrl(), null, false);
            oll = deserializeServiceType(ObjectLocationList.class, is);
            
        } catch (BaseException be) {
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

 		return oll;
	}

	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#get(org.dataone.service.types.v1.Identifier)
	 */
	@Override
	public InputStream get(Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		try {
			return super.get(pid);
		} catch (InsufficientResources e) {
			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#get(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier)
	 */
	@Override
	public InputStream get(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		try {
			return super.get(session, pid);
		} catch (InsufficientResources e) {
			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(e);
		}
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#getChecksum(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier)
	 */
	@Override
	public Checksum getChecksum(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		Checksum cs = null;
		try {
			cs = super.getChecksum(session, pid, null);
		} catch (InvalidRequest e) {
			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(e);
		}
    	return cs;
	}

	
	/**
	 *  A convenience method for creating a search command utilizing the D1Url
	 *  class for building the value for the query parameter. The class D1Url 
	 *  handles general URL escaping of individual url elements, but different
	 *  search implementations, such as solr, may have extra requirements.
	 *  
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.search">see DataONE API Reference</a> }
	 *  
	 *  solr escaping: {@link <a href="http://www.google.com/search?q=solr+escapequerychars+api">find ClientUtils</a> }
	 * 
	 * @param queryD1url - a D1Url object containing the path and/or query elements
	 *                     that will be passed to the indicated queryType.  BaseUrl
	 *                     and Resource segments contained in this object will be
	 *                     removed/ignored.
	 */
	public  ObjectList search(String queryType, D1Url queryD1url)
	throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest, 
	NotImplemented
	{
		return search(this.defaultSession, queryType, queryD1url);
	}
	
	
	/**
	 *  A convenience method for creating a search command utilizing the D1Url
	 *  class for building the value for the query parameter. The class D1Url 
	 *  handles general URL escaping of individual url elements, but different
	 *  search implementations, such as solr, may have extra requirements.
	 *  
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.search">see DataONE API Reference</a> }
	 *  
	 *  solr escaping: {@link <a href="http://www.google.com/search?q=solr+escapequerychars+api">find ClientUtils</a> }
	 * 
	 * @param queryD1url - a D1Url object containing the path and/or query elements
	 *                     that will be passed to the indicated queryType.  
	 */
	public  ObjectList search(Session session, String queryType, D1Url queryD1url)
	throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest, 
	NotImplemented
	{
		String pathAndQueryString = queryD1url.getUrl();
		return search(session, queryType, pathAndQueryString);
	}
	
	
	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.search">see DataONE API Reference</a> }
	 * 
	 * This implementation handles URL-escaping for only the "queryType" parameter,
	 * and always places a slash ('/') character after it.
	 * <p>
	 * For example, to invoke the following solr query:
	 * <pre>"?q=id:MyStuff:*&start=0&rows=10&fl=id score"</pre>
	 * 
	 * one has to (1) escape appropriate characters according to the rules of
	 * the queryType employed (in this case solr):
	 * <pre>  "?q=id\:MyStuff\:\*&start=0&rows=10&fl=id\ score"</pre>
	 *  
	 * then (2) escape according to general url rules:
	 * 
	 * <pre>  "?q=id%5C:MyStuff%5C:%5C*&start=0&rows=10&fl=id%5C%20score"</pre>
	 *
	 * resulting in: 
	 * <pre>cn.search(defaultSession,"solr","?q=id%5C:MyStuff%5C:%5C*&start=0&rows=10&fl=id%5C%20score")</pre> 
	 *  
	 *  For solr queries, a list of query terms employed can be found at the DataONE documentation on 
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/design/SearchMetadata.html"> Content Discovery</a> }
	 *  solr escaping: {@link <a href="http://www.google.com/search?q=solr+escapequerychars+api">find ClientUtils</a> }
	 */
	@Override
	public  ObjectList search(Session session, String queryType, String query)
			throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest, 
			NotImplemented
			{

        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_SEARCH);
        url.addNextPathElement(queryType);
        
        String finalUrl = url.getUrl() + "/" + "select" + query;

        ObjectList objectList = null;
        try {
            InputStream is = getRestClient(session).doGetRequest(finalUrl, null);
            objectList = deserializeServiceType(ObjectList.class, is);
            
		} catch (BaseException be) {
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

		return objectList;
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#setRightsHolder(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.Subject, long)
	 */
	@Override
	public  Identifier setRightsHolder(Session session, Identifier pid, Subject userId, 
			long serialVersion)
	throws InvalidToken, ServiceFailure, NotFound, NotAuthorized, NotImplemented, 
	InvalidRequest, VersionMismatch
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OWNER);
		if (pid == null)
			throw new InvalidRequest("0000","'pid' cannot be null");
		url.addNextPathElement(pid.getValue());
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();

    	if (userId == null)
    		throw new InvalidRequest("0000","parameter 'userId' cannot be null");
    	mpe.addParamPart("userId", userId.getValue());
    	mpe.addParamPart("serialVersion", String.valueOf(serialVersion));

		// send the request
		Identifier identifier = null;

		try {
			InputStream is = getRestClient(session).doPutRequest(url.getUrl(),mpe, null);
			identifier = deserializeServiceType(Identifier.class, is);
			
		} catch (BaseException be) {
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof VersionMismatch)        throw (VersionMismatch) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

		return identifier;
	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#setAccessPolicy(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.AccessPolicy, long)
	 */
	@Override
	public  boolean setAccessPolicy(Session session, Identifier pid, 
			AccessPolicy accessPolicy, long serialVersion) 
	throws InvalidToken, NotFound, NotImplemented, NotAuthorized, 
		ServiceFailure, InvalidRequest, VersionMismatch
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCESS);
		if (pid == null)
			throw new InvalidRequest("0000","'pid' cannot be null");
		url.addNextPathElement(pid.getValue());
		
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("accessPolicy", accessPolicy);
    		mpe.addParamPart("serialVersion", String.valueOf(serialVersion));
    	} catch (IOException e1) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
		}

		try {
			InputStream is = getRestClient(session).doPutRequest(url.getUrl(),mpe, null);
			if (is != null)
				is.close();

		} catch (BaseException be) {
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof VersionMismatch)         throw (VersionMismatch) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

		return true;
	}

	
	//////////  CN IDENTITY API  ///////////////
	
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#registerAccount(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Person)
	 */
	@Override
	public  Subject registerAccount(Session session, Person person) 
			throws ServiceFailure, NotAuthorized, IdentifierNotUnique, InvalidCredentials, 
			NotImplemented, InvalidRequest, InvalidToken
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNTS);
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("person", person);
    	} catch (IOException e1) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
		}

		Subject subject = null;
		try {
			InputStream is = getRestClient(session).doPostRequest(url.getUrl(),mpe, null);
			subject = deserializeServiceType(Subject.class, is);
			
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof IdentifierNotUnique)    throw (IdentifierNotUnique) be;
			if (be instanceof InvalidCredentials)     throw (InvalidCredentials) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof InvalidToken)	          throw (InvalidToken) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

		return subject;
	}

	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#updateAccount(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Person)
	 */
	@Override
	public  Subject updateAccount(Session session, Person person) 
			throws ServiceFailure, NotAuthorized, InvalidCredentials, 
			NotImplemented, InvalidRequest, InvalidToken, NotFound
	{
		if (person.getSubject() == null) {
			throw new NotFound("0000","'person.subject' cannot be null");
		}
		
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNTS);
    	
		url.addNextPathElement(person.getSubject().getValue());
		
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("person", person);
    	} catch (IOException e1) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
		}

		Subject subject = null;
		try {
			InputStream is = getRestClient(session).doPutRequest(url.getUrl(),mpe, null);
			subject = deserializeServiceType(Subject.class, is);
			
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof InvalidCredentials)     throw (InvalidCredentials) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof InvalidToken)	          throw (InvalidToken) be;
			if (be instanceof NotFound)               throw (NotFound) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

		return subject;
	}

	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#verifyAccount(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Subject)
	 */
	@Override
	public boolean verifyAccount(Session session, Subject subject) 
			throws ServiceFailure, NotAuthorized, NotImplemented, InvalidToken, 
			InvalidRequest
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_VERIFICATION);
		if (subject == null) {
			throw new InvalidRequest("0000","'subject' cannot be null");
		}
		url.addNextPathElement(subject.getValue());
		
		try {
			InputStream is = getRestClient(session).doPutRequest(url.getUrl(),null, null);
			if (is != null)
				is.close();
		
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		return true;
	}

	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#getSubjectInfo(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Subject)
	 */
	@Override
	public SubjectInfo getSubjectInfo(Session session, Subject subject)
	throws ServiceFailure, NotAuthorized, NotImplemented, NotFound, InvalidToken
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNTS);
		if (subject == null)
			throw new NotFound("0000","'subject' cannot be null");
		url.addNextPathElement(subject.getValue());


		SubjectInfo subjectInfo = null;

		try {
			InputStream is = getRestClient(session).doGetRequest(url.getUrl(), null);
			subjectInfo = deserializeServiceType(SubjectInfo.class, is);
			
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotFound)               throw (NotFound) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

		return subjectInfo;
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#listSubjects(org.dataone.service.types.v1.Session, String, String, Integer, Integer)
	 */
	@Override
	public SubjectInfo listSubjects(Session session, String query, String status, Integer start, 
			Integer count) throws InvalidRequest, ServiceFailure, InvalidToken, NotAuthorized, 
			NotImplemented
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNTS);
    	url.addNonEmptyParamPair("query", query);
    	url.addNonEmptyParamPair("status", status);
    	url.addNonEmptyParamPair("start", start);
    	url.addNonEmptyParamPair("count", count);
    	
		SubjectInfo subjectInfo = null;

		try {
			InputStream is = getRestClient(session).doGetRequest(url.getUrl(), null);
			subjectInfo = deserializeServiceType(SubjectInfo.class, is);
			
		} catch (BaseException be) {
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

		return subjectInfo;
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#mapIdentity(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Subject, org.dataone.service.types.v1.Subject)
	 */
	@Override
	public boolean mapIdentity(Session session, Subject primarySubject, Subject secondarySubject)
	throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented, InvalidRequest, IdentifierNotUnique
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING);
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();

    	if (primarySubject != null)
    		mpe.addParamPart("primarySubject", primarySubject.getValue());
    	if (secondarySubject != null)
    		mpe.addParamPart("secondarySubject", secondarySubject.getValue());

		try {
			InputStream is = getRestClient(session).doPostRequest(url.getUrl(),mpe, null);
			if (is != null)
				is.close();
			
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof IdentifierNotUnique)    throw (IdentifierNotUnique) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

		return true;
	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#requestMapIdentity(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Subject)
	 */
	@Override
	public boolean requestMapIdentity(Session session, Subject subject) 
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented, InvalidRequest, IdentifierNotUnique
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING_PENDING);
    	
		SimpleMultipartEntity mpe = new SimpleMultipartEntity();
		mpe.addParamPart("subject", subject.getValue());

		try {
			InputStream is = getRestClient(session).doPostRequest(url.getUrl(), mpe, null);
			if (is != null)
				is.close();
		
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof IdentifierNotUnique)    throw (IdentifierNotUnique) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		
		return true;
	}
    
    
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#getPendingMapIdentity(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Subject)
	 */
    @Override
	public SubjectInfo getPendingMapIdentity(Session session, Subject subject) 
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented 
    {  	
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING_PENDING);
    	url.addNextPathElement(subject.getValue());
    	
		SubjectInfo subjectInfo = null;

		try {
			InputStream is = getRestClient(session).doGetRequest(url.getUrl(), null);
			subjectInfo = deserializeServiceType(SubjectInfo.class, is);
			
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof InvalidToken)	          throw (InvalidToken) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

		return subjectInfo;
    }
	
	
    /* (non-Javadoc)
	 * @see org.dataone.client.CNode#confirmMapIdentity(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Subject)
	 */
	@Override
	public boolean confirmMapIdentity(Session session, Subject subject) 
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING_PENDING);
    	url.addNextPathElement(subject.getValue());

		try {
			InputStream is = getRestClient(session).doPutRequest(url.getUrl(), null, null);
			if (is != null)
				is.close();

		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		return true;
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#denyMapIdentity(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Subject)
	 */
	@Override
	public boolean denyMapIdentity(Session session, Subject subject) 
	throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING_PENDING);
    	url.addNextPathElement(subject.getValue());
		
		try {
			InputStream is = getRestClient(session).doDeleteRequest(url.getUrl(), null);
			if (is != null)
				is.close();

		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		
		return true;
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#removeMapIdentity(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Subject)
	 */
	@Override
	public  boolean removeMapIdentity(Session session, Subject subject) 
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING);
    	url.addNextPathElement(subject.getValue());

		try {
			InputStream is = getRestClient(session).doDeleteRequest(url.getUrl(), null);
			if (is != null)
				is.close();

		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		
		return true;
	}

	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#createGroup(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Group)
	 */
	@Override
	public Subject createGroup(Session session, Group group) 
	throws ServiceFailure, InvalidToken, NotAuthorized, NotImplemented, IdentifierNotUnique
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_GROUPS);
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
        	//url.addNextPathElement(group.getSubject().getValue());
    		mpe.addFilePart("group", group);
    	} catch (IOException e1) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
		}

		// send the request
		Subject subject = null;

		try {
			InputStream is = getRestClient(session).doPostRequest(url.getUrl(), mpe, null);
			subject = deserializeServiceType(Subject.class, is);
			
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof IdentifierNotUnique)    throw (IdentifierNotUnique) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

		return subject;
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#updateGroup(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Group)
	 */
	@Override
	public boolean updateGroup(Session session, Group group) 
		throws ServiceFailure, InvalidToken, 
			NotAuthorized, NotFound, NotImplemented, InvalidRequest
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_GROUPS);
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
        	//url.addNextPathElement(group.getSubject().getValue());
    		mpe.addFilePart("group", group);
    	} catch (IOException e1) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
		}

		try {
			InputStream is = getRestClient(session).doPutRequest(url.getUrl(),mpe, null);
			if (is != null)
				is.close();
			
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		
		return true;
	}


	///////////// CN REGISTER API   ////////////////
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#updateNodeCapabilities(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.NodeReference, org.dataone.service.types.v1.Node)
	 */
	@Override
	public boolean updateNodeCapabilities(Session session, NodeReference nodeid, Node node) 
	throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, NotFound, InvalidToken
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_NODE);
		if (nodeid != null)
			url.addNextPathElement(nodeid.getValue());
    	
		SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("node", node);
    	} catch (IOException e1) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
		}

		try {
			InputStream is = getRestClient(session).doPutRequest(url.getUrl(),mpe, null);
			if (is != null)
				is.close();

		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotFound)               throw (NotFound) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		
		return true;
	}



	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#register(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Node)
	 */
	@Override
	public NodeReference register(Session session, Node node)
	throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, 
	IdentifierNotUnique, InvalidToken
	{
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_NODE);
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("node", node);
    	} catch (IOException e1) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
		}

		NodeReference nodeRef = null;
		try {
			InputStream is = getRestClient(session).doPostRequest(url.getUrl(),mpe, null);
			nodeRef = deserializeServiceType(NodeReference.class, is);
			
		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof IdentifierNotUnique)    throw (IdentifierNotUnique) be;
			if (be instanceof InvalidToken)	          throw (InvalidToken) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

		return nodeRef;
	}


	////////////  CN REPLICATION API    ///////////////////
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#setReplicationStatus(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.NodeReference, org.dataone.service.types.v1.ReplicationStatus, org.dataone.service.exceptions.BaseException)
	 */
	@Override
	public boolean setReplicationStatus(Session session, Identifier pid, 
			NodeReference nodeRef, ReplicationStatus status, BaseException failure) 
					throws ServiceFailure, NotImplemented, InvalidToken, NotAuthorized, 
					InvalidRequest, NotFound
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_REPLICATION_NOTIFY);
		if (pid != null)
			url.addNextPathElement(pid.getValue());
		
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
        mpe.addParamPart("nodeRef", nodeRef.getValue());
        mpe.addParamPart("status", status.xmlValue());
        try {
            if ( failure != null ) {
                mpe.addFilePart("failure", failure.serialize(BaseException.FMT_XML));
            }
            
        } catch (IOException e1) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);        
        }

		try {
			InputStream is = getRestClient(session).doPutRequest(url.getUrl(),mpe,
					Settings.getConfiguration().getInteger("D1Client.CNode.replication.timeout", null));
			if (is != null) 
				is.close();
			
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof NotFound)               throw (NotFound) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		
		return true;
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#setReplicationPolicy(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.ReplicationPolicy, long)
	 */
	@Override
	public boolean setReplicationPolicy(Session session, Identifier pid, 
		ReplicationPolicy policy, long serialVersion) 
	throws NotImplemented, NotFound, NotAuthorized, ServiceFailure, 
		InvalidRequest, InvalidToken, VersionMismatch
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_REPLICATION_POLICY);
		if (pid != null)
			url.addNextPathElement(pid.getValue());
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("policy", policy);
    		mpe.addParamPart("serialVersion", String.valueOf(serialVersion));
    	} catch (IOException e1) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
		}

		try {
			InputStream is = getRestClient(session).doPutRequest(url.getUrl(),mpe,
					Settings.getConfiguration().getInteger("D1Client.CNode.replication.timeout", null));
			if (is != null) 
				is.close();
	
		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof VersionMismatch)         throw (VersionMismatch) be;
			
			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		
		return true;
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#isNodeAuthorized(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Subject, org.dataone.service.types.v1.Identifier)
	 */
	@Override
	public  boolean isNodeAuthorized(Session session, 
			Subject targetNodeSubject, Identifier pid)
	throws NotImplemented, NotAuthorized, InvalidToken, ServiceFailure, 
	NotFound, InvalidRequest
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_REPLICATION_AUTHORIZED);
        if (pid != null)
        	url.addNextPathElement(pid.getValue());
        if (targetNodeSubject != null)
        	url.addNonEmptyParamPair("targetNodeSubject", targetNodeSubject.getValue());

		try {
			InputStream is = getRestClient(session).doGetRequest(url.getUrl(),
					Settings.getConfiguration().getInteger("D1Client.CNode.replication.timeout", null));
			if (is != null) 
				is.close();

		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		
		return true;
	}

	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#updateReplicationMetadata(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.Replica, long)
	 */
	@Override
	public boolean updateReplicationMetadata(Session session, 
			Identifier pid, Replica replicaMetadata, long serialVersion)
	throws NotImplemented, NotAuthorized, ServiceFailure, NotFound, 
	InvalidRequest, InvalidToken, VersionMismatch
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_REPLICATION_META);
		if (pid != null)
			url.addNextPathElement(pid.getValue());
		

		SimpleMultipartEntity mpe = new SimpleMultipartEntity();

		try {
			mpe.addFilePart("replicaMetadata", replicaMetadata);
			mpe.addParamPart("serialVersion", String.valueOf(serialVersion));
		} catch (IOException e1) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
		}

		try {
			InputStream is = getRestClient(session).doPutRequest(url.getUrl(),mpe,
					Settings.getConfiguration().getInteger("D1Client.CNode.replication.timeout", null));
			if (is != null)
				is.close();

		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof VersionMismatch)        throw (VersionMismatch) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		
		return true;
	}

	
	/* (non-Javadoc)
	 * @see org.dataone.client.CNode#deleteReplicationMetadata(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.NodeReference, long)
	 */
	@Override
	public boolean deleteReplicationMetadata(Session session, Identifier pid,
			NodeReference nodeId, long serialVersion) throws InvalidToken,
			ServiceFailure, NotAuthorized, NotFound, NotImplemented,
			VersionMismatch, InvalidRequest {
		
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_REPLICATION_DELETE_REPLICA);
		if (pid != null) {
			url.addNextPathElement(pid.getValue());
		}
		
		SimpleMultipartEntity mpe = new SimpleMultipartEntity();
		if (nodeId != null)
			mpe.addParamPart("nodeId", nodeId.getValue());
		mpe.addParamPart("serialVersion", String.valueOf(serialVersion));

		try {
			InputStream is = getRestClient(session).doPutRequest(url.getUrl(),mpe,
					Settings.getConfiguration().getInteger("D1Client.CNode.replication.timeout", null));
			if (is != null)
				is.close();

		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof InvalidRequest)           throw (InvalidRequest) be;
			if (be instanceof VersionMismatch)         throw (VersionMismatch) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
		
		return true;
	}

	
	@Override
	public InputStream view(Session session, String theme, Identifier id) throws InvalidToken,
    ServiceFailure, NotAuthorized, InvalidRequest, NotImplemented, NotFound {
	    return super.view(session, theme, id);
	}
	
	
	@Override
	public OptionList listViews() throws InvalidToken, ServiceFailure, NotAuthorized,
    InvalidRequest, NotImplemented {
	    return super.listViews(null);
	}

//    @Override
    public SubjectInfo echoCredentials(Session session) throws NotImplemented, ServiceFailure,
            InvalidToken {

        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_DIAG_SUBJECT);

        SubjectInfo subjectInfo = null;
        try {
            InputStream is = getRestClient(session).doGetRequest(url.getUrl(),
                    Settings.getConfiguration().getInteger("D1Client.CNode.replication.timeout", null));
            subjectInfo = deserializeServiceType(SubjectInfo.class, is);

        } catch (BaseException be) {
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof InvalidToken)           throw (InvalidToken) be;

            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
        } 
        catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }
        
        return subjectInfo;
    }


//    @Override
    public SystemMetadata echoSystemMetadata(Session session, SystemMetadata sysmeta)
            throws NotImplemented, ServiceFailure, NotAuthorized, InvalidToken, InvalidRequest,
            IdentifierNotUnique, InvalidSystemMetadata {

        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_DIAG_SYSMETA);
        
        SystemMetadata echoedSysmeta = null;
        try {
            SimpleMultipartEntity mpe = new SimpleMultipartEntity();
            mpe.addFilePart("sysmeta", sysmeta);
            
            InputStream is = getRestClient(this.defaultSession).doPostRequest(url.getUrl(), mpe, null);
            echoedSysmeta = deserializeServiceType(SystemMetadata.class, is);
            
        } catch (BaseException be) {
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
            if (be instanceof InvalidSystemMetadata)  throw (InvalidSystemMetadata) be;
            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
            
        } catch (JiBXException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);  
        } catch (IOException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        } catch (ClientSideException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        }

        return echoedSysmeta;
    }


//    @Override
    public InputStream echoIndexedObject(Session session, String queryEngine,
            SystemMetadata sysmeta, InputStream object) throws NotImplemented, ServiceFailure,
            NotAuthorized, InvalidToken, InvalidRequest, InvalidSystemMetadata, UnsupportedType,
            UnsupportedMetadataType, InsufficientResources {

        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_DIAG_OBJECT);
        
        InputStream echoedObject = null;
        try {
            SimpleMultipartEntity mpe = new SimpleMultipartEntity();
            mpe.addFilePart("queryEngine", queryEngine);
            mpe.addFilePart("sysmeta", sysmeta);
            if (object == null) {
                mpe.addFilePart("object", "");
            } else {
                mpe.addFilePart("object", object);
            }
            
            echoedObject = getRestClient(this.defaultSession).doPostRequest(url.getUrl(), mpe, null);
            
        } catch (BaseException be) {
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
            if (be instanceof InvalidSystemMetadata)  throw (InvalidSystemMetadata) be;
            if (be instanceof UnsupportedType)        throw (UnsupportedType) be;
            if (be instanceof UnsupportedMetadataType)throw (UnsupportedMetadataType) be;
            if (be instanceof InsufficientResources)  throw (InsufficientResources) be;
            throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
            
        } catch (JiBXException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);  
        } catch (IOException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        } catch (ClientSideException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        }

        return echoedObject;
    }
}
