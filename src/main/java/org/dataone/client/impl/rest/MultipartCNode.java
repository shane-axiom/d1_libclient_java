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
package org.dataone.client.impl.rest;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.LogFactory;
import org.apache.http.client.params.ClientPNames;
import org.dataone.client.CNode;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.formats.ObjectFormatCache;
import org.dataone.client.utils.ExceptionUtils;
import org.dataone.configuration.Settings;
import org.dataone.mimemultipart.SimpleMultipartEntity;
import org.dataone.service.cn.v1.CNAuthorization;
import org.dataone.service.cn.v1.CNCore;
import org.dataone.service.cn.v1.CNIdentity;
import org.dataone.service.cn.v1.CNRead;
import org.dataone.service.cn.v1.CNRegister;
import org.dataone.service.cn.v1.CNReplication;
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
import org.dataone.service.exceptions.VersionMismatch;
import org.dataone.service.types.v1.AccessPolicy;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.ChecksumAlgorithmList;
import org.dataone.service.types.v1.DescribeResponse;
import org.dataone.service.types.v1.Event;
import org.dataone.service.types.v1.Group;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Log;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeList;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormat;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ObjectFormatList;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.ObjectLocationList;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Person;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationPolicy;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SubjectInfo;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.types.v1.util.NodelistUtil;
import org.dataone.service.types.v1_1.QueryEngineDescription;
import org.dataone.service.types.v1_1.QueryEngineList;
import org.dataone.service.util.Constants;
import org.dataone.service.util.D1Url;
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
public class MultipartCNode extends MultipartD1Node 
implements CNCore, CNRead, CNAuthorization, CNIdentity, CNRegister, CNReplication, CNode 
{
	protected static org.apache.commons.logging.Log log = LogFactory.getLog(MultipartCNode.class);

	private Map<String, String> nodeId2URLMap;
	private long lastNodeListRefreshTimeMS = 0;
    private Integer nodelistRefreshIntervalSeconds = 2 * 60;
	
 //   private static final String REPLICATION_TIMEOUT_PROPERTY = "D1Client.CNode.replication.timeout";
	
	/**
	 * Construct a Coordinating Node, passing in the base url for node services. The CN
	 * first retrieves a list of other nodes that can be used to look up node
	 * identifiers and base urls for further service invocations.
	 *
	 * @param nodeBaseServiceUrl base url for constructing service endpoints.
	 */
	public MultipartCNode(String nodeBaseServiceUrl) {
		super(nodeBaseServiceUrl);
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
	 * @param session - the Session object passed to the CertificateManager
     *                  to be used for establishing connections
	 */
	public MultipartCNode(String nodeBaseServiceUrl, Session session) {
		super(nodeBaseServiceUrl, session);
		nodelistRefreshIntervalSeconds = Settings.getConfiguration()
			.getInteger("CNode.nodemap.cache.refresh.interval.seconds", 
						nodelistRefreshIntervalSeconds);
	}

	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#getNodeBaseServiceUrl()
	 */
	@Override
	public String getNodeBaseServiceUrl() {
		D1Url url = new D1Url(super.getNodeBaseServiceUrl());
		url.addNextPathElement(CNCore.SERVICE_VERSION);
		return url.getUrl();
	}

	/**
     * Find the base URL for a Node based on the Node's identifier as it was 
     * registered with the Coordinating Node.  This method does the lookup on
     * cached NodeList information.  The cache is refreshed periodically.
     *  
     * @param nodeId the identifier value of the node to look up
     * @return the base URL of the node's service endpoints
     * @throws ServiceFailure 
     * @throws NotImplemented 
     */
    public String lookupNodeBaseUrl(String nodeId) throws ServiceFailure, NotImplemented {
    	
    	// prevents null pointer exception from being thrown at the map get(nodeId) call
    	if (nodeId == null) {
    		nodeId = "";
    	}
    	String url = null;
    	if (isNodeMapStale()) {
    		refreshNodeMap();           
    		url = nodeId2URLMap.get(nodeId);
    	} else {
    		url = nodeId2URLMap.get(nodeId);
    		if (url == null) {
    			// refresh the nodeMap, maybe that will help.
    			refreshNodeMap();
    			url = nodeId2URLMap.get(nodeId);
    		}
    	}
        return url;
    }
    
    /**
     * Find the base URL for a Node based on the Node's identifier as it was 
     * registered with the Coordinating Node.  This method does the lookup on
     * cached NodeList information.
     *  
     * @param nodeRef a NodeReference for the node to look up
     * @return the base URL of the node's service endpoints
     * @throws ServiceFailure 
     * @throws NotImplemented 
     */
    public String lookupNodeBaseUrl(NodeReference nodeRef) throws ServiceFailure, NotImplemented {
    	
    	// prevents null pointer exception from being thrown at the map get(nodeId) call
    	String nodeId = (nodeRef == null) ? "" : nodeRef.getValue();
    	if (nodeId == null)
    		nodeId = "";
    		
    	String url = null;
    	if (isNodeMapStale()) {
    		refreshNodeMap();           
    		url = nodeId2URLMap.get(nodeId);
    	} else {
    		url = nodeId2URLMap.get(nodeId);
    		if (url == null) {
    			// refresh the nodeMap, maybe that will help.
    			refreshNodeMap();
    			url = nodeId2URLMap.get(nodeId);
    		}
    	}
        return url;
    }
	
    /**
     * Find the node identifier for a Node based on the base URL that is used to
     * access its services by looking up the registration for the node at the 
     * Coordinating Node.  This method does the lookup on
     * cached NodeList information.
     * @param nodeBaseUrl the base url for Node service access
     * @return the identifier of the Node
     * @throws NotImplemented 
     * @throws ServiceFailure 
     */
    // TODO: check other packages to see if we can return null instead of empty string
    // (one dependency is d1_client_r D1Client)
    public String lookupNodeId(String nodeBaseUrl) throws ServiceFailure, NotImplemented {
        String nodeId = "";
        if (isNodeMapStale()) {
    		refreshNodeMap();
        }
        for (String key : nodeId2URLMap.keySet()) {
            if (nodeBaseUrl.equals(nodeId2URLMap.get(key))) {
                // We have a match, so record it and break
                nodeId = key;
                break;
            }
        }
        return nodeId;
    }

    /**
     * Initialize the map of nodes (pairs of NodeId/NodeUrl) by doing a listNodes() call
     * and mapping baseURLs to the Identifiers.  These values are used later to
     * look up a node's URL based on its ID, or its ID based on its URL.
     * @throws ServiceFailure 
     * @throws NotImplemented 
     */
    private void refreshNodeMap() throws ServiceFailure, NotImplemented
    { 		
    	nodeId2URLMap = NodelistUtil.mapNodeList(listNodes());
    }
    
    /* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#listNodeIds()
	 */
	public Set<String> listNodeIds() throws ServiceFailure, NotImplemented {
    	if (isNodeMapStale()) {
    		refreshNodeMap();
    	}
    	return nodeId2URLMap.keySet();
    }
	
    
    private boolean isNodeMapStale()
    {
    	if (nodeId2URLMap == null) 
    		return true;
    	
    	Date now = new Date();
    	long nowMS = now.getTime();
        DateFormat df = DateFormat.getDateTimeInstance();
        df.format(now);

        // convert seconds to milliseconds
        long refreshIntervalMS = this.nodelistRefreshIntervalSeconds * 1000L;
        if (nowMS - this.lastNodeListRefreshTimeMS > refreshIntervalMS) {
            this.lastNodeListRefreshTimeMS = nowMS;
            log.info("  CNode nodelist refresh: new cached time: " + df.format(now));
            return true;
        } else {
            return false;
        }
    }
    

    /* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#ping()
	 */
    @Override
	public Date ping() throws NotImplemented, ServiceFailure, InsufficientResources
    {
    	return super.ping();
    }
    
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#listFormats()
	 */
	@Override
	public  ObjectFormatList listFormats()
	throws ServiceFailure, NotImplemented
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_FORMATS);

		// send the request                
		ObjectFormatList formatList = null;
		
		try {			
			InputStream is = this.restClient.doGetRequest(url.getUrl());
			formatList = deserializeServiceType(ObjectFormatList.class, is);

		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

		return formatList;
	}


	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#getFormat(org.dataone.service.types.v1.ObjectFormatIdentifier)
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
				InputStream is = this.restClient.doGetRequest(url.getUrl());
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
	
	
    /* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#listChecksumAlgorithms()
	 */
	@Override
	public ChecksumAlgorithmList listChecksumAlgorithms() throws ServiceFailure, NotImplemented 
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_CHECKSUM);

		ChecksumAlgorithmList algorithmList = null;
		try {
			InputStream is = this.restClient.doGetRequest(url.getUrl());
			algorithmList = deserializeServiceType(ChecksumAlgorithmList.class, is);
		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;

			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientSideException e)  {throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e); }

		return algorithmList;
	}


	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#getLogRecords()
	 */
	@Override
	public  Log getLogRecords() 
	throws InvalidToken, InvalidRequest, ServiceFailure,
	NotAuthorized, NotImplemented, InsufficientResources
	{
		return super.getLogRecords();
	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#getLogRecords(org.dataone.service.types.v1.Session)
	 */
	@Override
	public  Log getLogRecords(Session session) 
	throws InvalidToken, InvalidRequest, ServiceFailure,
	NotAuthorized, NotImplemented, InsufficientResources
	{
		return super.getLogRecords(session);
	}
	


	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#getLogRecords(java.util.Date, java.util.Date, org.dataone.service.types.v1.Event, java.lang.String, java.lang.Integer, java.lang.Integer)
	 */
	@Override
	public  Log getLogRecords(Date fromDate, Date toDate,
			Event event, String pidFilter, Integer start, Integer count) 
	throws InvalidToken, InvalidRequest, ServiceFailure,
	NotAuthorized, NotImplemented, InsufficientResources
	{
		return super.getLogRecords(fromDate, toDate, event, pidFilter, start, count);
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#getLogRecords(org.dataone.service.types.v1.Session, java.util.Date, java.util.Date, org.dataone.service.types.v1.Event, java.lang.String, java.lang.Integer, java.lang.Integer)
	 */
	@Override
	public  Log getLogRecords(Session session, Date fromDate, Date toDate,
			Event event, String pidFilter, Integer start, Integer count) 
	throws InvalidToken, InvalidRequest, ServiceFailure,
	NotAuthorized, NotImplemented, InsufficientResources
	{
		return super.getLogRecords(session, fromDate, toDate, event, pidFilter, start, count);
	}
		

	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#listNodes()
	 */
    @Override
	public NodeList listNodes() throws NotImplemented, ServiceFailure
    {
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_NODE);

		// send the request
		NodeList nodelist = null;

		try {
			InputStream is = this.restClient.doGetRequest(url.getUrl());
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
	 * @see org.dataone.client.impl.rest.ICNode#reserveIdentifier(org.dataone.service.types.v1.Identifier)
	 */
	@Override
	public Identifier reserveIdentifier(Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, 
	NotImplemented, InvalidRequest
	{
		return reserveIdentifier(this.session, pid);
	}
    
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#reserveIdentifier(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier)
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
 			InputStream is = this.restClient.doPostRequest(url.getUrl(),smpe);
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
	 * @see org.dataone.client.impl.rest.ICNode#generateIdentifier(java.lang.String, java.lang.String)
	 */
	@Override
	public  Identifier generateIdentifier(String scheme, String fragment)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest
	{
		return super.generateIdentifier(scheme, fragment);
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#generateIdentifier(org.dataone.service.types.v1.Session, java.lang.String, java.lang.String)
	 */
	@Override
	public  Identifier generateIdentifier(Session session, String scheme, String fragment)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest
	{
		return super.generateIdentifier(session, scheme, fragment);
	}


	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#hasReservation(org.dataone.service.types.v1.Subject, org.dataone.service.types.v1.Identifier)
	 */
	@Override
	public boolean hasReservation(Subject subject, Identifier pid)
	throws InvalidToken, ServiceFailure,  NotFound, NotAuthorized, 
	NotImplemented, IdentifierNotUnique
	{
		return hasReservation(this.session, subject, pid);
	}
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#hasReservation(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Subject, org.dataone.service.types.v1.Identifier)
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
			InputStream is = this.restClient.doGetRequest(url.getUrl());
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
	 * @see org.dataone.client.impl.rest.ICNode#create(org.dataone.service.types.v1.Identifier, java.io.InputStream, org.dataone.service.types.v1.SystemMetadata)
	 */
	@Override
	public Identifier create(Identifier pid, InputStream object,
			SystemMetadata sysmeta) 
	throws InvalidToken, ServiceFailure,NotAuthorized, IdentifierNotUnique, UnsupportedType,
	InsufficientResources, InvalidSystemMetadata, NotImplemented, InvalidRequest
	{
		return create(this.session, pid, object, sysmeta);
	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#create(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, java.io.InputStream, org.dataone.service.types.v1.SystemMetadata)
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
        	InputStream is = this.restClient.doPostRequest(url.getUrl(), mpe);
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
	 * @see org.dataone.client.impl.rest.ICNode#registerSystemMetadata(org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.SystemMetadata)
	 */
	@Override
	public Identifier registerSystemMetadata( Identifier pid, SystemMetadata sysmeta) 
	throws NotImplemented, NotAuthorized,ServiceFailure, InvalidRequest, 
	InvalidSystemMetadata, InvalidToken
	{
		return registerSystemMetadata(this.session, pid,sysmeta);
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#registerSystemMetadata(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.SystemMetadata)
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
			InputStream is = this.restClient.doPostRequest(url.getUrl(),mpe);
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

	

	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#setObsoletedBy(org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.Identifier, long)
	 */
	@Override
	public boolean setObsoletedBy( Identifier pid,
			Identifier obsoletedByPid, long serialVersion)
			throws NotImplemented, NotFound, NotAuthorized, ServiceFailure,
			InvalidRequest, InvalidToken, VersionMismatch 
			{
		return setObsoletedBy(this.session, pid, obsoletedByPid, serialVersion);
	}

	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#setObsoletedBy(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.Identifier, long)
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
			InputStream is = this.restClient.doPutRequest(url.getUrl(), mpe);
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

	////////////////   CN READ API  //////////////

	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#listObjects()
	 */
	@Override
	public ObjectList listObjects() 
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure 
	{
		return super.listObjects();
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#listObjects(org.dataone.service.types.v1.Session)
	 */
	@Override
	public ObjectList listObjects(Session session) 
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure 
	{
		return super.listObjects(session);
	}
	
	

    /* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#listObjects(java.util.Date, java.util.Date, org.dataone.service.types.v1.ObjectFormatIdentifier, java.lang.Boolean, java.lang.Integer, java.lang.Integer)
	 */
	@Override
	public ObjectList listObjects(Date fromDate,
			Date toDate, ObjectFormatIdentifier formatid,
			Boolean replicaStatus, Integer start, Integer count)
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure 
	{
		return super.listObjects(fromDate,toDate,formatid,replicaStatus,start,count);
	}

	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#listObjects(org.dataone.service.types.v1.Session, java.util.Date, java.util.Date, org.dataone.service.types.v1.ObjectFormatIdentifier, java.lang.Boolean, java.lang.Integer, java.lang.Integer)
	 */
	@Override
	public ObjectList listObjects(Session session, Date fromDate,
			Date toDate, ObjectFormatIdentifier formatid,
			Boolean replicaStatus, Integer start, Integer count)
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure 
	{
		return super.listObjects(session,fromDate,toDate,formatid,replicaStatus,start,count);
	}
	

	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#get(org.dataone.service.types.v1.Identifier)
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
	 * @see org.dataone.client.impl.rest.ICNode#get(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier)
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
	 * @see org.dataone.client.impl.rest.ICNode#getSystemMetadata(org.dataone.service.types.v1.Identifier)
	 */
	@Override
	public SystemMetadata getSystemMetadata(Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		return super.getSystemMetadata(pid);
	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#getSystemMetadata(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier)
	 */
	@Override
	public SystemMetadata getSystemMetadata(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		return super.getSystemMetadata(session, pid);
	}


	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#describe(org.dataone.service.types.v1.Identifier)
	 */
    @Override
	public DescribeResponse describe(Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
    	return super.describe(pid);
    }
    
    /* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#describe(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier)
	 */
    @Override
	public DescribeResponse describe(Session session, Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
    	return super.describe(session,pid);
    }


	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#resolve(org.dataone.service.types.v1.Identifier)
	 */
	@Override
	public ObjectLocationList resolve(Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		return resolve(this.session, pid);
	}
    
    /* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#resolve(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier)
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
			// set flag to true to allow redirects (http.SEE_OTHER) to represent success
			InputStream is = this.restClient.doGetRequest(url.getUrl(),true);
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
	 * @see org.dataone.client.impl.rest.ICNode#getChecksum(org.dataone.service.types.v1.Identifier)
	 */
	@Override
	public Checksum getChecksum(Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		Checksum cs = null;
		try {
			cs = super.getChecksum(pid, null);
		} catch (InvalidRequest e) {
			throw ExceptionUtils.recastDataONEExceptionToServiceFailure(e);
		}
    	return cs;
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#getChecksum(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier)
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

	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#search(java.lang.String, org.dataone.service.util.D1Url)
	 */
	@Override
	public  ObjectList search(String queryType, D1Url queryD1url)
	throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest, 
	NotImplemented
	{
		return search(this.session, queryType, queryD1url);
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#search(org.dataone.service.types.v1.Session, java.lang.String, org.dataone.service.util.D1Url)
	 */
	@Override
	public  ObjectList search(Session session, String queryType, D1Url queryD1url)
	throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest, 
	NotImplemented
	{
		String pathAndQueryString = queryD1url.getUrl();
		return search(session, queryType, pathAndQueryString);
	}
	

	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#search(java.lang.String, java.lang.String)
	 */
	@Override
	public  ObjectList search(String queryType, String query)
			throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest, 
			NotImplemented
	{
		return search(this.session, queryType, query);
	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#search(org.dataone.service.types.v1.Session, java.lang.String, java.lang.String)
	 */
	@Override
	public  ObjectList search(Session session, String queryType, String query)
			throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest, 
			NotImplemented
			{

        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_SEARCH);
        url.addNextPathElement(queryType);
        
        String finalUrl = url.getUrl() + "/" + query;

        ObjectList objectList = null;
        try {
            InputStream is = this.restClient.doGetRequest(finalUrl);
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

	
	////////// CN Authorization API //////////////

	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#setRightsHolder(org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.Subject, long)
	 */
	@Override
	public  Identifier setRightsHolder(Identifier pid, Subject userId, 
			long serialVersion)
	throws InvalidToken, ServiceFailure, NotFound, NotAuthorized, NotImplemented, 
	InvalidRequest, VersionMismatch
	{
		return setRightsHolder(this.session, pid, userId, serialVersion);
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#setRightsHolder(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.Subject, long)
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
			InputStream is = this.restClient.doPutRequest(url.getUrl(),mpe);
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
	 * @see org.dataone.client.impl.rest.ICNode#isAuthorized(org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.Permission)
	 */
	@Override
	public boolean isAuthorized(Identifier pid, Permission permission)
	throws ServiceFailure, InvalidToken, NotFound, NotAuthorized, 
	NotImplemented, InvalidRequest
	{
		return super.isAuthorized(pid, permission);
	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#isAuthorized(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.Permission)
	 */
	@Override
	public boolean isAuthorized(Session session, Identifier pid, Permission permission)
	throws ServiceFailure, InvalidToken, NotFound, NotAuthorized, 
	NotImplemented, InvalidRequest
	{
		return super.isAuthorized(session, pid, permission);
	}


	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#setAccessPolicy(org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.AccessPolicy, long)
	 */
	@Override
	public  boolean setAccessPolicy(Identifier pid, 
			AccessPolicy accessPolicy, long serialVersion) 
	throws InvalidToken, NotFound, NotImplemented, NotAuthorized, 
		ServiceFailure, InvalidRequest, VersionMismatch
	{
		return setAccessPolicy(this.session, pid, accessPolicy, serialVersion);
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#setAccessPolicy(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.AccessPolicy, long)
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
			InputStream is = this.restClient.doPutRequest(url.getUrl(),mpe);
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
	 * @see org.dataone.client.impl.rest.ICNode#registerAccount(org.dataone.service.types.v1.Person)
	 */
	@Override
	public  Subject registerAccount(Person person) 
			throws ServiceFailure, NotAuthorized, IdentifierNotUnique, InvalidCredentials, 
			NotImplemented, InvalidRequest, InvalidToken
	{
		return registerAccount(this.session, person); 
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#registerAccount(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Person)
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
			InputStream is = this.restClient.doPostRequest(url.getUrl(),mpe);
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
	 * @see org.dataone.client.impl.rest.ICNode#updateAccount(org.dataone.service.types.v1.Person)
	 */
	@Override
	public  Subject updateAccount(Person person) 
			throws ServiceFailure, NotAuthorized, InvalidCredentials, 
			NotImplemented, InvalidRequest, InvalidToken, NotFound
	{
		return updateAccount(this.session, person);
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#updateAccount(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Person)
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
			InputStream is = this.restClient.doPutRequest(url.getUrl(),mpe);
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
	 * @see org.dataone.client.impl.rest.ICNode#verifyAccount(org.dataone.service.types.v1.Subject)
	 */
	@Override
	public boolean verifyAccount(Subject subject) 
			throws ServiceFailure, NotAuthorized, NotImplemented, InvalidToken, 
			InvalidRequest
	{
		return verifyAccount(this.session, subject);
	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#verifyAccount(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Subject)
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
			InputStream is = this.restClient.doPutRequest(url.getUrl(),null);
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
	 * @see org.dataone.client.impl.rest.ICNode#getSubjectInfo(org.dataone.service.types.v1.Subject)
	 */
	@Override
	public SubjectInfo getSubjectInfo(Subject subject)
	throws ServiceFailure, NotAuthorized, NotImplemented, NotFound, InvalidToken
	{
		return getSubjectInfo(this.session, subject);
	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#getSubjectInfo(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Subject)
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
			InputStream is = this.restClient.doGetRequest(url.getUrl());
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
	 * @see org.dataone.client.impl.rest.ICNode#listSubjects(java.lang.String, java.lang.String, java.lang.Integer, java.lang.Integer)
	 */
	@Override
	public SubjectInfo listSubjects(String query, String status, Integer start, 
			Integer count) throws InvalidRequest, ServiceFailure, InvalidToken, NotAuthorized, 
			NotImplemented
	{
		return listSubjects(this.session, query, status, start, count);
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#listSubjects(org.dataone.service.types.v1.Session, java.lang.String, java.lang.String, java.lang.Integer, java.lang.Integer)
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
			InputStream is = this.restClient.doGetRequest(url.getUrl());
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
	 * @see org.dataone.client.impl.rest.ICNode#mapIdentity(org.dataone.service.types.v1.Subject, org.dataone.service.types.v1.Subject)
	 */
	@Override
	public boolean mapIdentity(Subject primarySubject, Subject secondarySubject)
	throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented, InvalidRequest, IdentifierNotUnique
	{
		return mapIdentity(this.session, primarySubject, secondarySubject);
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#mapIdentity(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Subject, org.dataone.service.types.v1.Subject)
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
			InputStream is = this.restClient.doPostRequest(url.getUrl(),mpe);
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
	 * @see org.dataone.client.impl.rest.ICNode#requestMapIdentity(org.dataone.service.types.v1.Subject)
	 */
	@Override
	public boolean requestMapIdentity(Subject subject) 
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented, InvalidRequest, IdentifierNotUnique
	{
		return requestMapIdentity(this.session, subject);
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#requestMapIdentity(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Subject)
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
			InputStream is = this.restClient.doPostRequest(url.getUrl(), mpe);
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
	 * @see org.dataone.client.impl.rest.ICNode#getPendingMapIdentity(org.dataone.service.types.v1.Subject)
	 */
    @Override
	public SubjectInfo getPendingMapIdentity(Subject subject) 
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented 
    {  	
    	return getPendingMapIdentity(this.session, subject);
    }
    
    
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#getPendingMapIdentity(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Subject)
	 */
    @Override
	public SubjectInfo getPendingMapIdentity(Session session, Subject subject) 
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented 
    {  	
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING_PENDING);
    	url.addNextPathElement(subject.getValue());
    	
		SubjectInfo subjectInfo = null;

		try {
			InputStream is = this.restClient.doGetRequest(url.getUrl());
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
	 * @see org.dataone.client.impl.rest.ICNode#confirmMapIdentity(org.dataone.service.types.v1.Subject)
	 */
	@Override
	public boolean confirmMapIdentity(Subject subject) 
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented
	{
		return confirmMapIdentity(this.session, subject) ;
	}
	
	
    /* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#confirmMapIdentity(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Subject)
	 */
	@Override
	public boolean confirmMapIdentity(Session session, Subject subject) 
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING_PENDING);
    	url.addNextPathElement(subject.getValue());

		try {
			InputStream is = this.restClient.doPutRequest(url.getUrl(), null);
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
	 * @see org.dataone.client.impl.rest.ICNode#denyMapIdentity(org.dataone.service.types.v1.Subject)
	 */
	@Override
	public boolean denyMapIdentity(Subject subject) 
	throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented
	{
		return denyMapIdentity(this.session, subject);
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#denyMapIdentity(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Subject)
	 */
	@Override
	public boolean denyMapIdentity(Session session, Subject subject) 
	throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING_PENDING);
    	url.addNextPathElement(subject.getValue());
		
		try {
			InputStream is = this.restClient.doDeleteRequest(url.getUrl());
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
	 * @see org.dataone.client.impl.rest.ICNode#removeMapIdentity(org.dataone.service.types.v1.Subject)
	 */
	@Override
	public  boolean removeMapIdentity(Subject subject) 
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented
	{
		return removeMapIdentity(this.session, subject);
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#removeMapIdentity(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Subject)
	 */
	@Override
	public  boolean removeMapIdentity(Session session, Subject subject) 
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING);
    	url.addNextPathElement(subject.getValue());

		try {
			InputStream is = this.restClient.doDeleteRequest(url.getUrl());
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
	 * @see org.dataone.client.impl.rest.ICNode#createGroup(org.dataone.service.types.v1.Group)
	 */
	@Override
	public Subject createGroup(Group group) 
	throws ServiceFailure, InvalidToken, NotAuthorized, NotImplemented, IdentifierNotUnique
	{
		return createGroup(this.session, group);
	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#createGroup(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Group)
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
			InputStream is = this.restClient.doPostRequest(url.getUrl(), mpe);
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
	 * @see org.dataone.client.impl.rest.ICNode#updateGroup(org.dataone.service.types.v1.Group)
	 */
	@Override
	public boolean updateGroup(Group group) 
		throws ServiceFailure, InvalidToken, 
			NotAuthorized, NotFound, NotImplemented, InvalidRequest
	{
		return updateGroup(this.session, group);
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#updateGroup(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Group)
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
			InputStream is = this.restClient.doPutRequest(url.getUrl(),mpe);
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
	 * @see org.dataone.client.impl.rest.ICNode#updateNodeCapabilities(org.dataone.service.types.v1.NodeReference, org.dataone.service.types.v1.Node)
	 */
	@Override
	public boolean updateNodeCapabilities(NodeReference nodeid, Node node) 
	throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, NotFound, InvalidToken
	{
		return updateNodeCapabilities(this.session, nodeid, node);
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#updateNodeCapabilities(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.NodeReference, org.dataone.service.types.v1.Node)
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
			InputStream is = this.restClient.doPutRequest(url.getUrl(),mpe);
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
	 * @see org.dataone.client.impl.rest.ICNode#register(org.dataone.service.types.v1.Node)
	 */
	@Override
	public NodeReference register(Node node)
	throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, 
	IdentifierNotUnique, InvalidToken
	{
		return register(this.session, node);
	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#register(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Node)
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
			InputStream is = this.restClient.doPostRequest(url.getUrl(),mpe);
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


	////////////  CN REPLICATION API    ////////////////////
	

	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#setReplicationStatus(org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.NodeReference, org.dataone.service.types.v1.ReplicationStatus, org.dataone.service.exceptions.BaseException)
	 */
	@Override
	public boolean setReplicationStatus(Identifier pid, 
			NodeReference nodeRef, ReplicationStatus status, BaseException failure) 
					throws ServiceFailure, NotImplemented, InvalidToken, NotAuthorized, 
					InvalidRequest, NotFound
	{
		return setReplicationStatus(this.session, pid, nodeRef, status, failure);
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#setReplicationStatus(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.NodeReference, org.dataone.service.types.v1.ReplicationStatus, org.dataone.service.exceptions.BaseException)
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
			InputStream is = this.restClient.doPutRequest(url.getUrl(),mpe);
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
	 * @see org.dataone.client.impl.rest.ICNode#setReplicationPolicy(org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.ReplicationPolicy, long)
	 */
	@Override
	public boolean setReplicationPolicy(Identifier pid, ReplicationPolicy policy, long serialVersion) 
		throws NotImplemented, NotFound, NotAuthorized, ServiceFailure, 
		InvalidRequest, InvalidToken, VersionMismatch
	{
		return setReplicationPolicy(this.session, pid, policy, serialVersion);
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#setReplicationPolicy(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.ReplicationPolicy, long)
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
			InputStream is = this.restClient.doPutRequest(url.getUrl(),mpe);
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
	 * @see org.dataone.client.impl.rest.ICNode#isNodeAuthorized(org.dataone.service.types.v1.Subject, org.dataone.service.types.v1.Identifier)
	 */
	@Override
	public boolean isNodeAuthorized(Subject targetNodeSubject, Identifier pid)
	throws NotImplemented, NotAuthorized, InvalidToken, ServiceFailure, 
		NotFound, InvalidRequest
	{
		return isNodeAuthorized(this.session, targetNodeSubject, pid);
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#isNodeAuthorized(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Subject, org.dataone.service.types.v1.Identifier)
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
			InputStream is = this.restClient.doGetRequest(url.getUrl());
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
	 * @see org.dataone.client.impl.rest.ICNode#updateReplicationMetadata(org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.Replica, long)
	 */
	@Override
	public boolean updateReplicationMetadata( Identifier pid, Replica replicaMetadata, long serialVersion)
	throws NotImplemented, NotAuthorized, ServiceFailure, NotFound, 
		InvalidRequest, InvalidToken, VersionMismatch
	{
		return updateReplicationMetadata(this.session, pid, replicaMetadata, serialVersion);
	}

	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#updateReplicationMetadata(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.Replica, long)
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
			InputStream is = this.restClient.doPutRequest(url.getUrl(),mpe);
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
	 * @see org.dataone.client.impl.rest.ICNode#deleteReplicationMetadata(org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.NodeReference, long)
	 */
	@Override
	public boolean deleteReplicationMetadata(Identifier pid, NodeReference nodeId, long serialVersion) 
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented,
			VersionMismatch, InvalidRequest 
	{
		return deleteReplicationMetadata(this.session, pid,nodeId, serialVersion);
	}
	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#deleteReplicationMetadata(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier, org.dataone.service.types.v1.NodeReference, long)
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
			InputStream is = this.restClient.doPutRequest(url.getUrl(),mpe);
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

	
    /* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#archive(org.dataone.service.types.v1.Identifier)
	 */
    @Override
	public  Identifier archive(Identifier pid)
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
    {
        return  super.archive(pid);
    }
   
      
    /* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#archive(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier)
	 */
    @Override
	public  Identifier archive(Session session, Identifier pid)
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
    {
    	return super.archive(session, pid);
    }
     
	
	
    /* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#delete(org.dataone.service.types.v1.Identifier)
	 */
	@Override
    public Identifier delete(Identifier pid) throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented {
        return super.delete(pid);
    }
    
    /* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#delete(org.dataone.service.types.v1.Session, org.dataone.service.types.v1.Identifier)
	 */
	@Override
    public Identifier delete(Session session, Identifier pid) throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented {
        return super.delete(session, pid);
    }

    /* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#query(java.lang.String, java.lang.String)
	 */   
	@Override
	public InputStream query(String queryEngine, String query)
	throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
	NotImplemented, NotFound 
	{
		return super.query(queryEngine, query);
	}

    /* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#query(java.lang.String, org.dataone.service.util.D1Url)
	 */   
	@Override
	public InputStream query(String queryEngine, D1Url queryD1Url)
	throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
	NotImplemented, NotFound 
	{
		return super.query(queryEngine, queryD1Url.getUrl());
	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#getQueryEngineDescription(java.lang.String)
	 */
	@Override
	public QueryEngineDescription getQueryEngineDescription(String queryEngine)
    throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, NotFound 
	{
		return super.getQueryEngineDescription(queryEngine);
	}

	/* (non-Javadoc)
	 * @see org.dataone.client.impl.rest.ICNode#listQueryEngines()
	 */
	@Override
	public QueryEngineList listQueryEngines() 
	throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented 
	{
		return super.listQueryEngines();
	}


}
