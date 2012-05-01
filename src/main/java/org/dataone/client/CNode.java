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
import java.text.DateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.params.ClientPNames;
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
 */
public class CNode extends D1Node 
implements CNCore, CNRead, CNAuthorization, CNIdentity, CNRegister, CNReplication 
{
	protected static org.apache.commons.logging.Log log = LogFactory.getLog(CNode.class);

	private Map<String, String> nodeId2URLMap;
	private long lastNodeListRefreshTimeMS = 0;
    private Integer nodelistRefreshIntervalSeconds = 2 * 60;
	
	
	/**
	 * Construct a Coordinating Node, passing in the base url for node services. The CN
	 * first retrieves a list of other nodes that can be used to look up node
	 * identifiers and base urls for further service invocations.
	 *
	 * @param nodeBaseServiceUrl base url for constructing service endpoints.
	 */
	public CNode(String nodeBaseServiceUrl) {
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
	public CNode(String nodeBaseServiceUrl, Session session) {
		super(nodeBaseServiceUrl, session);
		nodelistRefreshIntervalSeconds = Settings.getConfiguration()
			.getInteger("CNode.nodemap.cache.refresh.interval.seconds", 
						nodelistRefreshIntervalSeconds);
	}

	
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
    

    /**
     *  {@link <a href="http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.ping">see DataONE API Reference</a> }
     */
    public Date ping() throws NotImplemented, ServiceFailure, InsufficientResources
    {
    	return super.ping();
    }
    
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.listFormats">see DataONE API Reference</a> } 
	 */
	public  ObjectFormatList listFormats()
	throws ServiceFailure, NotImplemented
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_FORMATS);

		// send the request
		D1RestClient client = new D1RestClient();
		ObjectFormatList formatList = null;
		
		try {			
			InputStream is = client.doGetRequest(url.getUrl());
			formatList = deserializeServiceType(ObjectFormatList.class, is);

		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return formatList;
	}


	/**
	 *  Return the ObjectFormat for the given ObjectFormatIdentifier, obtained 
	 *  either from a client-cached ObjectFormatList from the ObjectFormatCache,
	 *  or from a call to the CN.
	 *  Caching is enabled by default in production via the property 
	 *  "CNode.useObjectFormatCache" accessed via org.dataone.configuration.Settings
	 *  
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.getFormat">see DataONE API Reference</a> } 
	 */
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

				throw recastDataONEExceptionToServiceFailure(be);
			} 

		} else {
			D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_FORMATS);    
			url.addNextPathElement(formatid.getValue());

			// send the request
			D1RestClient client = new D1RestClient();

			try {
				InputStream is = client.doGetRequest(url.getUrl());
				objectFormat = deserializeServiceType(ObjectFormat.class, is);

			} catch (BaseException be) {
				if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
				if (be instanceof NotFound)               throw (NotFound) be;
				if (be instanceof NotImplemented)         throw (NotImplemented) be;

				throw recastDataONEExceptionToServiceFailure(be);
			} 
			catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
			catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
			catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
			catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

			finally {
				setLatestRequestUrl(client.getLatestRequestUrl());
				client.closeIdleConnections();
			}

		}
		return objectFormat;

	}
	
	
    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.getChecksumAlgorithms">see DataONE API Reference</a> } 
     */
	public ChecksumAlgorithmList listChecksumAlgorithms() throws ServiceFailure, NotImplemented 
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_CHECKSUM);

		D1RestClient client = new D1RestClient();

		ChecksumAlgorithmList algorithmList = null;
		try {
			InputStream is = client.doGetRequest(url.getUrl());
			algorithmList = deserializeServiceType(ChecksumAlgorithmList.class, is);
		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 
	
		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return algorithmList;
	}


	/**
	 *  A convenience method for getLogRecords using no filtering parameters
	 *  
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.getLogRecords">see DataONE API Reference</a> } 
	 */
	public  Log getLogRecords() 
	throws InvalidToken, InvalidRequest, ServiceFailure,
	NotAuthorized, NotImplemented, InsufficientResources
	{
		return super.getLogRecords(null);
	}
	
	/**
	 *  A convenience method for getLogRecords using no filtering parameters
	 *  
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.getLogRecords">see DataONE API Reference</a> } 
	 */
	public  Log getLogRecords(Session session) 
	throws InvalidToken, InvalidRequest, ServiceFailure,
	NotAuthorized, NotImplemented, InsufficientResources
	{
		return super.getLogRecords(session);
	}
	


	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.getLogRecords">see DataONE API Reference</a> } 
	 */
	public  Log getLogRecords(Date fromDate, Date toDate,
			Event event, String pidFilter, Integer start, Integer count) 
	throws InvalidToken, InvalidRequest, ServiceFailure,
	NotAuthorized, NotImplemented, InsufficientResources
	{
		return super.getLogRecords(D1Node.sessionFromConstructor(), fromDate, toDate, event, pidFilter, start, count);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.getLogRecords">see DataONE API Reference</a> } 
	 */
	public  Log getLogRecords(Session session, Date fromDate, Date toDate,
			Event event, String pidFilter, Integer start, Integer count) 
	throws InvalidToken, InvalidRequest, ServiceFailure,
	NotAuthorized, NotImplemented, InsufficientResources
	{
		return super.getLogRecords(session, fromDate, toDate, event, pidFilter, start, count);
	}
		

	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.listNodes">see DataONE API Reference</a> } 
	 */
    public NodeList listNodes() throws NotImplemented, ServiceFailure
    {
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_NODE);

		// send the request
		D1RestClient client = new D1RestClient();
		NodeList nodelist = null;

		try {
			InputStream is = client.doGetRequest(url.getUrl());
			nodelist = deserializeServiceType(NodeList.class, is);
			
		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 
	
		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return nodelist;
	}


	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.reserveIdentifier">see DataONE API Reference</a> } 
	 */
	public Identifier reserveIdentifier(Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, 
	NotImplemented, InvalidRequest
	{
		return reserveIdentifier(D1Node.sessionFromConstructor(), pid);
	}
    
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.reserveIdentifier">see DataONE API Reference</a> } 
	 */
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
		
		// send the request
		D1RestClient client = new D1RestClient(determineSession(session));
		Identifier identifier = null;
 		try {
 			InputStream is = client.doPostRequest(url.getUrl(),smpe);
 			identifier = deserializeServiceType(Identifier.class, is);
 			
		} catch (BaseException be) {
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof IdentifierNotUnique)    throw (IdentifierNotUnique) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
 		return identifier;
	}



	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.generateIdentifier">see DataONE API Reference</a> } 
	 */
	public  Identifier generateIdentifier(String scheme, String fragment)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest
	{
		return super.generateIdentifier(D1Node.sessionFromConstructor(), scheme, fragment);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.generateIdentifier">see DataONE API Reference</a> } 
	 */
	public  Identifier generateIdentifier(Session session, String scheme, String fragment)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest
	{
		return super.generateIdentifier(session, scheme, fragment);
	}


	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.hasReservation">see DataONE API Reference</a> } 
	 */
	public boolean hasReservation(Subject subject, Identifier pid)
	throws InvalidToken, ServiceFailure,  NotFound, NotAuthorized, 
	NotImplemented, IdentifierNotUnique
	{
		return hasReservation(D1Node.sessionFromConstructor(), subject, pid);
	}
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.hasReservation">see DataONE API Reference</a> } 
	 */
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
			throw recastClientSideExceptionToServiceFailure(e);
		}
    	
		// send the request
		D1RestClient client = new D1RestClient(determineSession(session));
		
		try {
			InputStream is = client.doGetRequest(url.getUrl());
			if (is != null)
				is.close();
			
		} catch (BaseException be) {
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof IdentifierNotUnique)    throw (IdentifierNotUnique) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return true;
	}


	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.create">see DataONE API Reference</a> } 
	 */
	public Identifier create(Identifier pid, InputStream object,
			SystemMetadata sysmeta) 
	throws InvalidToken, ServiceFailure,NotAuthorized, IdentifierNotUnique, UnsupportedType,
	InsufficientResources, InvalidSystemMetadata, NotImplemented, InvalidRequest
	{
		return create(D1Node.sessionFromConstructor(), pid, object, sysmeta);
	}
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.create">see DataONE API Reference</a> } 
	 */
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
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (JiBXException e) {
			throw recastClientSideExceptionToServiceFailure(e);	
		}

        D1RestClient client = new D1RestClient(determineSession(session));
        Identifier identifier = null;

        try {
        	InputStream is = client.doPostRequest(url.getUrl(), mpe);
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

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 
      
        finally {
        	setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
 		return identifier;
	}


	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.registerSystemMetadata">see DataONE API Reference</a> } 
	 */
	public Identifier registerSystemMetadata( Identifier pid, SystemMetadata sysmeta) 
	throws NotImplemented, NotAuthorized,ServiceFailure, InvalidRequest, 
	InvalidSystemMetadata, InvalidToken
	{
		return registerSystemMetadata(D1Node.sessionFromConstructor(), pid,sysmeta);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.registerSystemMetadata">see DataONE API Reference</a> } 
	 */
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
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(determineSession(session));

		Identifier identifier = null;
		try {
			InputStream is = client.doPostRequest(url.getUrl(),mpe);
			identifier = deserializeServiceType(Identifier.class, is);
		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof InvalidSystemMetadata)  throw (InvalidSystemMetadata) be;
			if (be instanceof InvalidToken)	          throw (InvalidToken) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
 		return identifier;
	}

	

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.setObsoletedBy">see DataONE API Reference</a> } 
	 */
	public boolean setObsoletedBy( Identifier pid,
			Identifier obsoletedByPid, long serialVersion)
			throws NotImplemented, NotFound, NotAuthorized, ServiceFailure,
			InvalidRequest, InvalidToken, VersionMismatch 
			{
		return setObsoletedBy(D1Node.sessionFromConstructor(), pid, obsoletedByPid, serialVersion);
	}

	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.setObsoletedBy">see DataONE API Reference</a> } 
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

		D1RestClient client = new D1RestClient(determineSession(session));

		try {
			InputStream is = client.doPutRequest(url.getUrl(), mpe);
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

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return true;
	}

	////////////////   CN READ API  //////////////

	/**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.listObjects">see DataONE API Reference</a> }
     */
	public ObjectList listObjects() 
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure 
	{
		return super.listObjects(null);
	}
	
	
	/**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.listObjects">see DataONE API Reference</a> }
     */
	@Override
	public ObjectList listObjects(Session session) 
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure 
	{
		return super.listObjects(session);
	}
	
	

    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.listObjects">see DataONE API Reference</a> }
     */
	public ObjectList listObjects(Date fromDate,
			Date toDate, ObjectFormatIdentifier formatid,
			Boolean replicaStatus, Integer start, Integer count)
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure 
	{
		return super.listObjects(D1Node.sessionFromConstructor(),fromDate,toDate,formatid,replicaStatus,start,count);
	}

	
	/**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.listObjects">see DataONE API Reference</a> }
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
	

	public InputStream get(Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
			return get(D1Node.sessionFromConstructor(), pid);
	}
	
	public InputStream get(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		try {
			return super.get(session, pid);
		} catch (InsufficientResources e) {
			throw recastDataONEExceptionToServiceFailure(e);
		}
	}



	public SystemMetadata getSystemMetadata(Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		return super.getSystemMetadata(D1Node.sessionFromConstructor(), pid);
	}
	
	public SystemMetadata getSystemMetadata(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		return super.getSystemMetadata(session, pid);
	}


	
	/**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.describe">see DataONE API Reference</a> } 
     */
    public DescribeResponse describe(Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
    	return super.describe(D1Node.sessionFromConstructor(),pid);
    }
    
    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.describe">see DataONE API Reference</a> } 
     */
    public DescribeResponse describe(Session session, Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
    	return super.describe(session,pid);
    }


	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.resolve">see DataONE API Reference</a> } 
	 */
	public ObjectLocationList resolve(Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		return resolve(D1Node.sessionFromConstructor(), pid);
	}
    
    /**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.resolve">see DataONE API Reference</a> } 
	 */
	public ObjectLocationList resolve(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_RESOLVE);
		if (pid == null) {
			throw new NotFound("0000", "'pid' cannot be null");
		}
        url.addNextPathElement(pid.getValue());

		// send the request
		D1RestClient client = new D1RestClient(determineSession(session));
		client.getHttpClient().getParams().setParameter(ClientPNames.HANDLE_REDIRECTS, false);
		ObjectLocationList oll = null;

		try {
			// set flag to true to allow redirects (http.SEE_OTHER) to represent success
			InputStream is = client.doGetRequest(url.getUrl(),true);
			oll = deserializeServiceType(ObjectLocationList.class, is);
			
		} catch (BaseException be) {
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 
	
		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
 		return oll;
	}


	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.getChecksum">see DataONE API Reference</a> } 
	 */
	public Checksum getChecksum(Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		return getChecksum(D1Node.sessionFromConstructor(), pid);
	}
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.getChecksum">see DataONE API Reference</a> } 
	 */
	public Checksum getChecksum(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		if (pid == null)
            throw new NotFound("0000", "PID cannot be null");

        // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_CHECKSUM);
    	url.addNextPathElement(pid.getValue());

		// send the request
		D1RestClient client = new D1RestClient(determineSession(session));
		Checksum checksum = null;

		try {
			InputStream is = client.doGetRequest(url.getUrl());
			checksum = deserializeServiceType(Checksum.class, is);
			
		} catch (BaseException be) {
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 
	
		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return checksum;
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
		return search(D1Node.sessionFromConstructor(), queryType, queryD1url);
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
	public  ObjectList search(Session session, String queryType, D1Url queryD1url)
	throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest, 
	NotImplemented
	{
		queryD1url.setBaseUrl("base");
		queryD1url.setResource("resource");
		String pathAndQueryString = queryD1url.getUrl().replaceAll("^base/resource/{0,1}", "");
		
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
	 * <pre>cn.search(session,"solr","?q=id%5C:MyStuff%5C:%5C*&start=0&rows=10&fl=id%5C%20score")</pre> 
	 *  
	 *  For solr queries, a list of query terms employed can be found at the DataONE documentation on 
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/design/SearchMetadata.html"> Content Discovery</a> }
     *  solr escaping: {@link <a href="http://www.google.com/search?q=solr+escapequerychars+api">find ClientUtils</a> }
	 */
	public  ObjectList search(String queryType, String query)
			throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest, 
			NotImplemented
	{
		return search(D1Node.sessionFromConstructor(), queryType, query);
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
	 * <pre>cn.search(session,"solr","?q=id%5C:MyStuff%5C:%5C*&start=0&rows=10&fl=id%5C%20score")</pre> 
	 *  
	 *  For solr queries, a list of query terms employed can be found at the DataONE documentation on 
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/design/SearchMetadata.html"> Content Discovery</a> }
     *  solr escaping: {@link <a href="http://www.google.com/search?q=solr+escapequerychars+api">find ClientUtils</a> }
	 */
	public  ObjectList search(Session session, String queryType, String query)
			throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest, 
			NotImplemented
			{

        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_SEARCH);
        url.addNextPathElement(queryType);
        
        String finalUrl = url.getUrl() + "/" + query;
        
        D1RestClient client = new D1RestClient(determineSession(session));

        ObjectList objectList = null;
        try {
            InputStream is = client.doGetRequest(finalUrl);
            objectList = deserializeServiceType(ObjectList.class, is);
            
		} catch (BaseException be) {
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 
	
		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return objectList;
	}

	
	////////// CN Authorization API //////////////

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNAuthorization.setRightsHolder">see DataONE API Reference</a> }
	 */
	public  Identifier setRightsHolder(Identifier pid, Subject userId, 
			long serialVersion)
	throws InvalidToken, ServiceFailure, NotFound, NotAuthorized, NotImplemented, 
	InvalidRequest, VersionMismatch
	{
		return setRightsHolder(D1Node.sessionFromConstructor(), pid, userId, serialVersion);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNAuthorization.setRightsHolder">see DataONE API Reference</a> }
	 */
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
		D1RestClient client = new D1RestClient(determineSession(session));
		Identifier identifier = null;

		try {
			InputStream is = client.doPutRequest(url.getUrl(),mpe);
			identifier = deserializeServiceType(Identifier.class, is);
			
		} catch (BaseException be) {
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof VersionMismatch)        throw (VersionMismatch) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return identifier;
	}


	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNAuthorization.isAuthorized">see DataONE API Reference</a> } 
	 */
	public boolean isAuthorized(Identifier pid, Permission permission)
	throws ServiceFailure, InvalidToken, NotFound, NotAuthorized, 
	NotImplemented, InvalidRequest
	{
		return super.isAuthorized(D1Node.sessionFromConstructor(), pid, permission);
	}
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNAuthorization.isAuthorized">see DataONE API Reference</a> } 
	 */
	public boolean isAuthorized(Session session, Identifier pid, Permission permission)
	throws ServiceFailure, InvalidToken, NotFound, NotAuthorized, 
	NotImplemented, InvalidRequest
	{
		return super.isAuthorized(session, pid, permission);
	}


	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNAuthorization.setAccessPolicy">see DataONE API Reference</a> } 
	 */
	public  boolean setAccessPolicy(Identifier pid, 
			AccessPolicy accessPolicy, long serialVersion) 
	throws InvalidToken, NotFound, NotImplemented, NotAuthorized, 
		ServiceFailure, InvalidRequest, VersionMismatch
	{
		return setAccessPolicy(D1Node.sessionFromConstructor(), pid, accessPolicy, serialVersion);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNAuthorization.setAccessPolicy">see DataONE API Reference</a> } 
	 */
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
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(determineSession(session));

		try {
			InputStream is = client.doPutRequest(url.getUrl(),mpe);
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

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return true;
	}

	
	//////////  CN IDENTITY API  ///////////////
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.registerAccount">see DataONE API Reference</a> } 
	 */
	public  Subject registerAccount(Person person) 
			throws ServiceFailure, NotAuthorized, IdentifierNotUnique, InvalidCredentials, 
			NotImplemented, InvalidRequest, InvalidToken
	{
		return registerAccount(D1Node.sessionFromConstructor(), person); 
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.registerAccount">see DataONE API Reference</a> } 
	 */
	public  Subject registerAccount(Session session, Person person) 
			throws ServiceFailure, NotAuthorized, IdentifierNotUnique, InvalidCredentials, 
			NotImplemented, InvalidRequest, InvalidToken
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNTS);
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("person", person);
    	} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(determineSession(session));

		Subject subject = null;
		try {
			InputStream is = client.doPostRequest(url.getUrl(),mpe);
			subject = deserializeServiceType(Subject.class, is);
			
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof IdentifierNotUnique)    throw (IdentifierNotUnique) be;
			if (be instanceof InvalidCredentials)     throw (InvalidCredentials) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof InvalidToken)	          throw (InvalidToken) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 
	
		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return subject;
	}


	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.updateAccount">see DataONE API Reference</a> } 
	 */
	public  Subject updateAccount(Person person) 
			throws ServiceFailure, NotAuthorized, InvalidCredentials, 
			NotImplemented, InvalidRequest, InvalidToken, NotFound
	{
		return updateAccount(D1Node.sessionFromConstructor(), person);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.updateAccount">see DataONE API Reference</a> } 
	 */
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
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(determineSession(session));

		Subject subject = null;
		try {
			InputStream is = client.doPutRequest(url.getUrl(),mpe);
			subject = deserializeServiceType(Subject.class, is);
			
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof InvalidCredentials)     throw (InvalidCredentials) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof InvalidToken)	          throw (InvalidToken) be;
			if (be instanceof NotFound)               throw (NotFound) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return subject;
	}


	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.verifyAccount">see DataONE API Reference</a> } 
	 */
	public boolean verifyAccount(Subject subject) 
			throws ServiceFailure, NotAuthorized, NotImplemented, InvalidToken, 
			InvalidRequest
	{
		return verifyAccount(D1Node.sessionFromConstructor(), subject);
	}
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.verifyAccount">see DataONE API Reference</a> } 
	 */
	public boolean verifyAccount(Session session, Subject subject) 
			throws ServiceFailure, NotAuthorized, NotImplemented, InvalidToken, 
			InvalidRequest
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_VERIFICATION);
		if (subject == null) {
			throw new InvalidRequest("0000","'subject' cannot be null");
		}
		url.addNextPathElement(subject.getValue());
		
        D1RestClient client = new D1RestClient(determineSession(session));

		try {
			InputStream is = client.doPutRequest(url.getUrl(),null);
			if (is != null)
				is.close();
		
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 
		
		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return true;
	}


	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.getSubjectInfo">see DataONE API Reference</a> } 
	 */
	public SubjectInfo getSubjectInfo(Subject subject)
	throws ServiceFailure, NotAuthorized, NotImplemented, NotFound, InvalidToken
	{
		return getSubjectInfo(D1Node.sessionFromConstructor(), subject);
	}
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.getSubjectInfo">see DataONE API Reference</a> } 
	 */
	public SubjectInfo getSubjectInfo(Session session, Subject subject)
	throws ServiceFailure, NotAuthorized, NotImplemented, NotFound, InvalidToken
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNTS);
		if (subject == null)
			throw new NotFound("0000","'subject' cannot be null");
		url.addNextPathElement(subject.getValue());

		D1RestClient client = new D1RestClient(determineSession(session));
		SubjectInfo subjectInfo = null;

		try {
			InputStream is = client.doGetRequest(url.getUrl());
			subjectInfo = deserializeServiceType(SubjectInfo.class, is);
			
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotFound)               throw (NotFound) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 
	
		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return subjectInfo;
	}


	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.listSubjects">see DataONE API Reference</a> } 
	 */
	public SubjectInfo listSubjects(String query, String status, Integer start, 
			Integer count) throws InvalidRequest, ServiceFailure, InvalidToken, NotAuthorized, 
			NotImplemented
	{
		return listSubjects(D1Node.sessionFromConstructor(), query, status, start, count);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.listSubjects">see DataONE API Reference</a> } 
	 */
	public SubjectInfo listSubjects(Session session, String query, String status, Integer start, 
			Integer count) throws InvalidRequest, ServiceFailure, InvalidToken, NotAuthorized, 
			NotImplemented
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNTS);
    	url.addNonEmptyParamPair("query", query);
    	url.addNonEmptyParamPair("status", status);
    	url.addNonEmptyParamPair("start", start);
    	url.addNonEmptyParamPair("count", count);
    	
		D1RestClient client = new D1RestClient(determineSession(session));
		SubjectInfo subjectInfo = null;

		try {
			InputStream is = client.doGetRequest(url.getUrl());
			subjectInfo = deserializeServiceType(SubjectInfo.class, is);
			
		} catch (BaseException be) {
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 
		
		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return subjectInfo;
	}


	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.mapIdentity">see DataONE API Reference</a> }
	 */
	public boolean mapIdentity(Subject primarySubject, Subject secondarySubject)
	throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented, InvalidRequest, IdentifierNotUnique
	{
		return mapIdentity(D1Node.sessionFromConstructor(), primarySubject, secondarySubject);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.mapIdentity">see DataONE API Reference</a> }
	 */
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
			
		D1RestClient client = new D1RestClient(determineSession(session));

		try {
			InputStream is = client.doPostRequest(url.getUrl(),mpe);
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

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return true;
	}


	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.requestMapIdentity">see DataONE API Reference</a> } 
	 */
	public boolean requestMapIdentity(Subject subject) 
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented, InvalidRequest, IdentifierNotUnique
	{
		return requestMapIdentity(D1Node.sessionFromConstructor(), subject);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.requestMapIdentity">see DataONE API Reference</a> } 
	 */
	public boolean requestMapIdentity(Session session, Subject subject) 
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented, InvalidRequest, IdentifierNotUnique
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING_PENDING);
    	
		SimpleMultipartEntity mpe = new SimpleMultipartEntity();
		mpe.addParamPart("subject", subject.getValue());

		D1RestClient client = new D1RestClient(determineSession(session));

		try {
			InputStream is = client.doPostRequest(url.getUrl(), mpe);
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

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return true;
	}


	
	/**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.getPendingMapIdentity">see DataONE API Reference</a> }
     */
    public SubjectInfo getPendingMapIdentity(Subject subject) 
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented 
    {  	
    	return getPendingMapIdentity(D1Node.sessionFromConstructor(), subject);
    }
    
    
	/**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.getPendingMapIdentity">see DataONE API Reference</a> }
     */
    public SubjectInfo getPendingMapIdentity(Session session, Subject subject) 
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented 
    {  	
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING_PENDING);
    	url.addNextPathElement(subject.getValue());
    	
		D1RestClient client = new D1RestClient(determineSession(session));
		SubjectInfo subjectInfo = null;

		try {
			InputStream is = client.doGetRequest(url.getUrl());
			subjectInfo = deserializeServiceType(SubjectInfo.class, is);
			
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof InvalidToken)	          throw (InvalidToken) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 
		
		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return subjectInfo;
    }
    

    
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.confirmMapIdentity">see DataONE API Reference</a> } 
	 */
	public boolean confirmMapIdentity(Subject subject) 
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented
	{
		return confirmMapIdentity(D1Node.sessionFromConstructor(), subject) ;
	}
	
	
    /**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.confirmMapIdentity">see DataONE API Reference</a> } 
	 */
	public boolean confirmMapIdentity(Session session, Subject subject) 
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING_PENDING);
    	url.addNextPathElement(subject.getValue());

		D1RestClient client = new D1RestClient(determineSession(session));

		try {
			InputStream is = client.doPutRequest(url.getUrl(), null);
			if (is != null)
				is.close();

		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return true;
	}


	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.denyMapIdentity">see DataONE API Reference</a> } 
	 */
	public boolean denyMapIdentity(Subject subject) 
	throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented
	{
		return denyMapIdentity(D1Node.sessionFromConstructor(), subject);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.denyMapIdentity">see DataONE API Reference</a> } 
	 */
	public boolean denyMapIdentity(Session session, Subject subject) 
	throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING_PENDING);
    	url.addNextPathElement(subject.getValue());
		D1RestClient client = new D1RestClient(determineSession(session));
		
		try {
			InputStream is = client.doDeleteRequest(url.getUrl());
			if (is != null)
				is.close();

		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return true;
	}


	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.removeMapIdentity">see DataONE API Reference</a> } 
	 */
	public  boolean removeMapIdentity(Subject subject) 
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented
	{
		return removeMapIdentity(D1Node.sessionFromConstructor(), subject);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.removeMapIdentity">see DataONE API Reference</a> } 
	 */
	public  boolean removeMapIdentity(Session session, Subject subject) 
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING);
    	url.addNextPathElement(subject.getValue());
		D1RestClient client = new D1RestClient(determineSession(session));

		try {
			InputStream is = client.doDeleteRequest(url.getUrl());
			if (is != null)
				is.close();

		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return true;
	}


	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.createGroup">see DataONE API Reference</a> } 
	 */
	public Subject createGroup(Group group) 
	throws ServiceFailure, InvalidToken, NotAuthorized, NotImplemented, IdentifierNotUnique
	{
		return createGroup(D1Node.sessionFromConstructor(), group);
	}
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.createGroup">see DataONE API Reference</a> } 
	 */
	public Subject createGroup(Session session, Group group) 
	throws ServiceFailure, InvalidToken, NotAuthorized, NotImplemented, IdentifierNotUnique
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_GROUPS);
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
        	//url.addNextPathElement(group.getSubject().getValue());
    		mpe.addFilePart("group", group);
    	} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		// send the request
		D1RestClient client = new D1RestClient(determineSession(session));
		Subject subject = null;

		try {
			InputStream is = client.doPostRequest(url.getUrl(), mpe);
			subject = deserializeServiceType(Subject.class, is);
			
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof IdentifierNotUnique)    throw (IdentifierNotUnique) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 
		
		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return subject;
	}


	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.addGroupMembers">see DataONE API Reference</a> } 
	 */
	public boolean updateGroup(Group group) 
		throws ServiceFailure, InvalidToken, 
			NotAuthorized, NotFound, NotImplemented, InvalidRequest
	{
		return updateGroup(D1Node.sessionFromConstructor(), group);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.addGroupMembers">see DataONE API Reference</a> } 
	 */
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
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(determineSession(session));

		try {
			InputStream is = client.doPutRequest(url.getUrl(),mpe);
			if (is != null)
				is.close();
			
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return true;
	}


	///////////// CN REGISTER API   ////////////////

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRegister.updateNodeCapabilities">see DataONE API Reference</a> } 
	 */
	public boolean updateNodeCapabilities(NodeReference nodeid, Node node) 
	throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, NotFound, InvalidToken
	{
		return updateNodeCapabilities(D1Node.sessionFromConstructor(), nodeid, node);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRegister.updateNodeCapabilities">see DataONE API Reference</a> } 
	 */
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
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(determineSession(session));

		try {
			InputStream is = client.doPutRequest(url.getUrl(),mpe);
			if (is != null)
				is.close();

		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotFound)               throw (NotFound) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 
		
		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return true;
	}


	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRegister.register">see DataONE API Reference</a> }
	 */
	public NodeReference register(Node node)
	throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, 
	IdentifierNotUnique, InvalidToken
	{
		return register(D1Node.sessionFromConstructor(), node);
	}
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRegister.register">see DataONE API Reference</a> }
	 */
	public NodeReference register(Session session, Node node)
	throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, 
	IdentifierNotUnique, InvalidToken
	{
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_NODE);
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("node", node);
    	} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(determineSession(session));

		NodeReference nodeRef = null;
		try {
			InputStream is = client.doPostRequest(url.getUrl(),mpe);
			nodeRef = deserializeServiceType(NodeReference.class, is);
			
		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof IdentifierNotUnique)    throw (IdentifierNotUnique) be;
			if (be instanceof InvalidToken)	          throw (InvalidToken) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 
		
		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return nodeRef;
	}


	////////////  CN REPLICATION API    ////////////////////
	

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.setReplicationStatus">see DataONE API Reference</a> }
	 */
	public boolean setReplicationStatus(Identifier pid, 
			NodeReference nodeRef, ReplicationStatus status, BaseException failure) 
					throws ServiceFailure, NotImplemented, InvalidToken, NotAuthorized, 
					InvalidRequest, NotFound
	{
		return setReplicationStatus(D1Node.sessionFromConstructor(), pid, nodeRef, status, failure);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.setReplicationStatus">see DataONE API Reference</a> }
	 */
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
            throw recastClientSideExceptionToServiceFailure(e1);        
        }

		D1RestClient client = new D1RestClient(determineSession(session));

		try {
			InputStream is = client.doPutRequest(url.getUrl(),mpe);
			if (is != null) 
				is.close();
			
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof NotFound)               throw (NotFound) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return true;
	}


	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.setReplicationPolicy">see DataONE API Reference</a> }
	 */
	public boolean setReplicationPolicy(Identifier pid, ReplicationPolicy policy, long serialVersion) 
		throws NotImplemented, NotFound, NotAuthorized, ServiceFailure, 
		InvalidRequest, InvalidToken, VersionMismatch
	{
		return setReplicationPolicy(D1Node.sessionFromConstructor(), pid, policy, serialVersion);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.setReplicationPolicy">see DataONE API Reference</a> }
	 */
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
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(determineSession(session));

		try {
			InputStream is = client.doPutRequest(url.getUrl(),mpe);
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
			
			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return true;
	}


	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.isNodeAuthorized">see DataONE API Reference</a> }
	 */
	public boolean isNodeAuthorized(Subject targetNodeSubject, Identifier pid)
	throws NotImplemented, NotAuthorized, InvalidToken, ServiceFailure, 
		NotFound, InvalidRequest
	{
		return isNodeAuthorized(D1Node.sessionFromConstructor(), targetNodeSubject, pid);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.isNodeAuthorized">see DataONE API Reference</a> }
	 */
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

        D1RestClient client = new D1RestClient(determineSession(session));

		try {
			InputStream is = client.doGetRequest(url.getUrl());
			if (is != null) 
				is.close();

		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return true;
	}


	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.updateReplicationMetadata">see DataONE API Reference</a> }
	 */
	public boolean updateReplicationMetadata( Identifier pid, Replica replicaMetadata, long serialVersion)
	throws NotImplemented, NotAuthorized, ServiceFailure, NotFound, 
		InvalidRequest, InvalidToken, VersionMismatch
	{
		return updateReplicationMetadata(D1Node.sessionFromConstructor(), pid, replicaMetadata, serialVersion);
	}

	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.updateReplicationMetadata">see DataONE API Reference</a> }
	 */
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
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(determineSession(session));
	
		try {
			InputStream is = client.doPutRequest(url.getUrl(),mpe);
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

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return true;
	}


	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.deleteReplicationMetadata">see DataONE API Reference</a> }
	 * @throws InvalidRequest 
	 */
	public boolean deleteReplicationMetadata(Identifier pid, NodeReference nodeId, long serialVersion) 
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented,
			VersionMismatch, InvalidRequest 
	{
		return deleteReplicationMetadata(D1Node.sessionFromConstructor(), pid,nodeId, serialVersion);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.deleteReplicationMetadata">see DataONE API Reference</a> }
	 * @throws InvalidRequest 
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

		D1RestClient client = new D1RestClient(determineSession(session));

		try {
			InputStream is = client.doPutRequest(url.getUrl(),mpe);
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

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return true;
	}

	
    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#CNCore.archive">see DataONE API Reference</a> }
     */
    public  Identifier archive( Identifier pid)
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
    {
        return  this.archive(D1Node.sessionFromConstructor(),  pid);
    }
   
    
    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#CNCore.archive.archive">see DataONE API Reference</a> }
     */
    public  Identifier archive(Session session, Identifier pid)
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
    {
        Identifier identifier = null;
    	try {
            identifier = super.archive(session, pid);
        } catch (InvalidRequest be) {
            //MN should not return this, but if it does recast as ServiceFailure
            throw new ServiceFailure("1350", be.getMessage());
        }
        return identifier;
    }
    
    
 
	
	
    @Override
    public Identifier delete(Identifier pid) throws InvalidToken, ServiceFailure, InvalidRequest, NotAuthorized, NotFound, NotImplemented {
        return this.delete(D1Node.sessionFromConstructor(), pid);
    }
}
