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
import java.util.Date;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;
import org.apache.http.client.ClientProtocolException;
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
import org.dataone.service.types.v1.Event;
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
import org.dataone.service.types.v1.SubjectList;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.types.v1.util.NodelistUtil;
import org.dataone.service.util.Constants;
import org.dataone.service.util.D1Url;
import org.jibx.runtime.JiBXException;

/**
 * CNode represents a DataONE Coordinating Node, and allows calling classes to
 * execute CN services.
 */
public class CNode extends D1Node 
implements CNCore, CNRead, CNAuthorization, CNIdentity, CNRegister, CNReplication 
{
	protected static org.apache.commons.logging.Log log = LogFactory.getLog(CNode.class);

	private Map<String, String> nodeId2URLMap;

	/**
	 * Construct a Coordinating Node, passing in the base url for node services. The CN
	 * first retrieves a list of other nodes that can be used to look up node
	 * identifiers and base urls for further service invocations.
	 *
	 * @param nodeBaseServiceUrl base url for constructing service endpoints.
	 */
	public CNode(String nodeBaseServiceUrl) {
		super(nodeBaseServiceUrl);
	}

	public String getNodeBaseServiceUrl() {
		D1Url url = new D1Url(super.getNodeBaseServiceUrl());
		url.addNextPathElement(CNCore.SERVICE_VERSION);
		return url.getUrl();
	}

	/**
     * Find the base URL for a Node based on the Node's identifier as it was registered with the
     * Coordinating Node. 
     * @param nodeId the identifier of the node to look up
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
    	if (nodeId2URLMap == null) {
    		initializeNodeMap();           
    		url = nodeId2URLMap.get(nodeId);
    	} else {
    		url = nodeId2URLMap.get(nodeId);
    		if (url == null) {
    			// refresh the nodeMap, maybe that will help.
    			initializeNodeMap();
    			url = nodeId2URLMap.get(nodeId);
    		}
    	}
        return url;
    }
	
    /**
     * Find the node identifier for a Node based on the base URL that is used to access its services
     * by looking up the registration for the node at the Coordinating Node.
     * @param nodeBaseUrl the base url for Node service access
     * @return the identifier of the Node
     */
    public String lookupNodeId(String nodeBaseUrl) {
        String nodeId = "";
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
    private void initializeNodeMap() throws ServiceFailure, NotImplemented
    {
    	try {
			InputStream inputStream = fetchNodeList();   		
    		nodeId2URLMap = NodelistUtil.mapNodeList(inputStream);
		} catch (IOException e) {
			recastClientSideExceptionToServiceFailure(e);
		} catch (InstantiationException e) {
			recastClientSideExceptionToServiceFailure(e);
		} catch (IllegalAccessException e) {
			recastClientSideExceptionToServiceFailure(e);
		} catch (JiBXException e) {
			recastClientSideExceptionToServiceFailure(e);
		}
    }
	

	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_core.listFormats */

	public  ObjectFormatList listFormats()
	throws ServiceFailure, InsufficientResources, NotImplemented
	{
		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_FORMATS);

		// send the request
		D1RestClient client = new D1RestClient();
		InputStream is = null;

		try {			
			is = client.doGetRequest(url.getUrl());
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InsufficientResources)  throw (InsufficientResources) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		return deserializeServiceType(ObjectFormatList.class, is);
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_core.getFormat */

	public  ObjectFormat getFormat(ObjectFormatIdentifier formatid)
	throws ServiceFailure, NotFound, InsufficientResources, NotImplemented
	{
		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_FORMATS);   	
    	url.addNextPathElement(formatid.getValue());
    	
		// send the request
		D1RestClient client = new D1RestClient();
		InputStream is = null;

		try {
			is = client.doGetRequest(url.getUrl());
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof InsufficientResources)  throw (InsufficientResources) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		return deserializeServiceType(ObjectFormat.class, is);
	}
	
	
	// TODO: uncomment method when return type available
//  /* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_core.getChecksumAlgorithms */
//	
//	public ChecksumAlgorithmList getChecksumAlgorithms() throws ServiceFailure, NotImplemented 
//	{
//		// TODO: create JavaDoc and fix doc reference
//
//		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_CHECKSUM);
//
//		D1RestClient client = new D1RestClient();
//
//		InputStream is = null;
//		try {
//			is = client.doGetRequest(url.getUrl());
//		} catch (BaseException be) {
//			if (be instanceof NotImplemented)         throw (NotImplemented) be;
//			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
//
//			throw recastDataONEExceptionToServiceFailure(be);
//		} 
//		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
//		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
//		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
//		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 
//		
//		// TODO Auto-generated method stub
//		return deserializeServiceType(ChecksumAlgorithmList.class, is);
//	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_core.getLogRecords */

	public  Log getLogRecords(Session session, Date fromDate, Date toDate,
			Event event, Integer start, Integer count) 
	throws InvalidToken, InvalidRequest, ServiceFailure,
	NotAuthorized, NotImplemented, InsufficientResources
	{
		return super.getLogRecords(session, fromDate, toDate, event, start, count);
	}
		


	
	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_core.listNodes */
	
    public NodeList listNodes() throws NotImplemented, ServiceFailure {
    	// the actual call is delegated to fetchNodeList, because the call is also used
    	// in the context of initializeNodeMap(), which needs the input stream
    	
    	// TODO: create JavaDoc and fix doc reference
    	
    	InputStream is = fetchNodeList();
    	return deserializeServiceType(NodeList.class, is);
    }
    
    
    private InputStream fetchNodeList() throws NotImplemented, ServiceFailure 
    {
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_NODE);

		// send the request
		D1RestClient client = new D1RestClient();
		InputStream is = null;

		try {
			is = client.doGetRequest(url.getUrl());
		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		return is;
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_core.reserveIdentifier */

	public Identifier reserveIdentifier(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, 
	NotImplemented, InvalidRequest
	{
		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_RESERVE);
		SimpleMultipartEntity smpe = new SimpleMultipartEntity();
		if (pid == null || pid.getValue().trim().equals("")) {
			throw new InvalidRequest("0000","PID cannot be null or whitespace string");
		}
		smpe.addParamPart("pid", pid.getValue());

		// send the request
		D1RestClient client = new D1RestClient(session);
		InputStream is = null;
 		try {
			is = client.doPostRequest(url.getUrl(),smpe);
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

		return deserializeServiceType(Identifier.class, is);
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_core.generateIdentifier */

	public  Identifier generateIdentifier(Session session, String scheme, String fragment)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest
	{
		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_GENERATE);
		SimpleMultipartEntity smpe = new SimpleMultipartEntity();
		if (scheme == null || scheme.trim().equals("")) {
			throw new InvalidRequest("0000","'scheme' cannot be null or whitespace string");
		}
		smpe.addParamPart("scheme", scheme);
		if (fragment != null) {
			smpe.addParamPart("fragment", fragment);
		}
		// send the request
		D1RestClient client = new D1RestClient(session);
		InputStream is = null;

		try {
			is = client.doPostRequest(url.getUrl(),smpe);
		} catch (BaseException be) {
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		return deserializeServiceType(Identifier.class, is);
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_core.hasReservation */

	public boolean hasReservation(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure,  NotFound, NotAuthorized, 
	NotImplemented, IdentifierNotUnique
	{
		// TODO: create JavaDoc and fix doc reference
		
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_RESERVE);
		if (pid == null || pid.getValue().trim().equals("")) {
			throw new NotFound("0000", "PID cannot be null or empty string or whitespace");
		}
		url.addNextPathElement(pid.getValue());

		// send the request
		D1RestClient client = new D1RestClient(session);

		try {
			client.doGetRequest(url.getUrl());
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

		return true;
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_core.create */

	public Identifier create(Session session, Identifier pid, InputStream object,
			SystemMetadata sysmeta) 
	throws InvalidToken, ServiceFailure,NotAuthorized, IdentifierNotUnique, UnsupportedType,
	InsufficientResources, InvalidSystemMetadata, NotImplemented, InvalidRequest
	{

		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
        url.addNextPathElement(pid.getValue());

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
        	mpe.addFilePart("sysmeta", sysmeta);
        } catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (JiBXException e) {
			throw recastClientSideExceptionToServiceFailure(e);	
		}

        D1RestClient client = new D1RestClient(session);
        InputStream is = null;

        try {
            is = client.doPostRequest(url.getUrl(), mpe);
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


		return deserializeServiceType(Identifier.class, is);
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_core.registerSystemMetadata */

	public Identifier registerSystemMetadata(Session session, Identifier pid,
		SystemMetadata sysmeta) 
	throws NotImplemented, NotAuthorized,ServiceFailure, InvalidRequest, InvalidSystemMetadata
	{

		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_META);
    	url.addNextPathElement(pid.getValue());
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("sysmeta", sysmeta);
    	} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(session);

		InputStream is = null;
		try {
			is = client.doPostRequest(url.getUrl(),mpe);
		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof InvalidSystemMetadata)  throw (InvalidSystemMetadata) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		return deserializeServiceType(Identifier.class, is);
	}


//	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_core.updateSystemMetadata */
//
//	public boolean updateSystemMetadata(Session session, Identifier pid) 
//	throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest,
//	InvalidSystemMetadata, NotFound
//	{
//
//		// TODO: create JavaDoc and fix doc reference
//
//		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_META);
//    	url.addNextPathElement(pid.getValue());
//    	
//    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
//    	try {
//    		mpe.addFilePart("sysmeta", sysmeta);
//    	} catch (IOException e1) {
//			throw recastClientSideExceptionToServiceFailure(e1);
//		} catch (JiBXException e1) {
//			throw recastClientSideExceptionToServiceFailure(e1);
//		}
//
//		D1RestClient client = new D1RestClient(session);
//
//		InputStream is = null;
//		try {
//			is = client.doPutRequest(url.getUrl(),mpe);
//		} catch (BaseException be) {
//			if (be instanceof NotImplemented)         throw (NotImplemented) be;
//			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
//			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
//			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
//			if (be instanceof InvalidSystemMetadata)  throw (InvalidSystemMetadata) be;
//			if (be instanceof NotFound)               throw (NotFound) be;
//
//			throw recastDataONEExceptionToServiceFailure(be);
//		} 
//		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
//		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
//		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
//		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 
//
//		// TODO: check return statement
//		return true;
//	}

	////////////////   CN READ API  //////////////

	@Override
	public ObjectList listObjects(Session session) 
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure 
	{
		return super.listObjects(session);
	}
	
	@Override
	public ObjectList listObjects(Session session, Date startTime,
			Date endTime, ObjectFormatIdentifier formatid,
			Boolean replicaStatus, Integer start, Integer count)
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure 
	{
		return super.listObjects(session,startTime,endTime,formatid,replicaStatus,start,count);
	}
	
	
	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_read.get */

	public InputStream get(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		return super.get(session, pid);
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_read.getSystemMetadata */

	public SystemMetadata getSystemMetadata(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		return super.getSystemMetadata(session, pid);
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_read.resolve */

	public ObjectLocationList resolve(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{

		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_RESOLVE);
        url.addNextPathElement(pid.getValue());


		// send the request
		D1RestClient client = new D1RestClient(session);
		InputStream is = null;

		try {
			is = client.doGetRequest(url.getUrl());
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

		return deserializeServiceType(ObjectLocationList.class, is);
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_read.assertRelation */
	@Deprecated
	public boolean assertRelation(Session session, Identifier pidOfSubject, 
	    String relationship, Identifier pidOfObject) throws InvalidToken, 
		ServiceFailure, NotAuthorized, NotFound, InvalidRequest, NotImplemented
	{

		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_RELATIONSHIP);
    	url.addNextPathElement(pidOfSubject.getValue());
    	url.addNonEmptyParamPair("relationship", relationship);
    	url.addNonEmptyParamPair("pidOfObject", pidOfObject.getValue());
    	
		// send the request
		D1RestClient client = new D1RestClient(session);

		try {
			client.doGetRequest(url.getUrl());
		} catch (BaseException be) {
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		return true;
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_read.getChecksum */

	public Checksum getChecksum(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		// TODO: create JavaDoc and fix doc reference

		if(pid == null || pid.getValue().trim().equals(""))
            throw new NotFound("0000", "PID cannot be null nor empty");

        // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_CHECKSUM);
    	url.addNextPathElement(pid.getValue());

		// send the request
		D1RestClient client = new D1RestClient(session);
		InputStream is = null;

		try {
			is = client.doGetRequest(url.getUrl());
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

		return deserializeServiceType(Checksum.class, is);
	}

	
	/**
     * See cn.search(Session session, String queryType, String query)
     * This is the same method but accepts a D1Url containing the query elements
     * that will be used for the query.
     * 
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_read.search
     * 
     * @param session
     * @param queryType  
     * @param query - D1Url object containing the query elements
     * @return - an ObjectList
     * @throws InvalidToken
     * @throws ServiceFailure
     * @throws NotAuthorized
     * @throws InvalidRequest
     * @throws NotImplemented
     * 
     */
    public ObjectList search(Session session, String queryType, D1Url query)
    throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
    NotImplemented
	{
    	return search(session,queryType,query.getAssembledQueryString());
	}
        
    /**
     * The CN implements two types of search: SOLR, and the DataONE native search
     * against the underlying data store. In contrast to the default behavior of 
     * other methods, character encoding is not performed on either the queryType
     * or query parameters, so the caller needs to send in a url-safe-encoded string.
     * <br>
     * One option for encoding is to use the org.dataone.service.util.D1Url class to 
     * encode and assemble the query string.
     * <p>
     *  For SOLR queries, a list of query terms can be found at:
     *  @see http://mule1.dataone.org/ArchitectureDocs-current/design/SearchMetadata.html
     *  and query syntax description can be found at:
     *  @see http://lucene.apache.org/java/2_4_0/queryparsersyntax.html and
     *  @see http://wiki.apache.org/solr/SolrQuerySyntax
     *  for example:  query = "q=replica_verified:[* TO NOW]&start=0&rows=10&fl=id%2Cscore
     *       replica_verified%3A%5B*+TO+NOW%5D
     * <p>
     * For DataONE type queries, the same parameters are available as are for mn.listObjects():
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.listObjects
     * for example:  query = "objectFormt=text/csv&start=0&count=25"
     *  
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_read.search 
     * 
     *  @param session - the session object.  If null, will default to 'public'
     *  @param queryType - the type of query passed in on the query parameter.
     *                    possible values: SOLR, null
     *                    null will direct to the DataONE native search
     *                    SOLR will redirect to the CN's SOLR index
     *  @param query -  the query string passed used to form the specific query, following
     *                  the syntax of the query type, and pre-encoded 
     *                  
     *  @return - an ObjectList
     */
	public  ObjectList search(Session session, String queryType, String query)
			throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest, 
			NotImplemented
			{

		// TODO: do we need to create a public user?
//		if (session == null) {
//            session = new Session();
//            session.setSubject(new Subject());
//            session.getSubject().setValue("public");
//        }
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);

        // set default params, if need be
        String paramAdditions = "";
        if ((queryType != null) && !queryType.isEmpty()) {
          paramAdditions = "qt=" + queryType +"&";
        }
        if (query == null) query = "";
 
        String paramsComplete = paramAdditions + query;
        // clean up paramsComplete string
        if (paramsComplete.endsWith("&")) {
            paramsComplete = paramsComplete.substring(0, paramsComplete.length() - 1);
        }

        url.addPreEncodedNonEmptyQueryParams(paramsComplete);

        D1RestClient client = new D1RestClient(session);

        InputStream is = null;
        try {
            is = client.doGetRequest(url.getUrl());
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

		return deserializeServiceType(ObjectList.class, is);
	}

	
	////////// CN Authorization API //////////////
	
	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_authorization.setOwner */

	public  Identifier setOwner(Session session, Identifier pid, Subject userId, 
			long serialVersion)
	throws InvalidToken, ServiceFailure, NotFound, NotAuthorized, NotImplemented, 
	InvalidRequest, VersionMismatch
	{

		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING);
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("pid", pid);
    		mpe.addFilePart("userId", userId);
    		mpe.addParamPart("serialVersion", String.valueOf(serialVersion));
    	} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (JiBXException e) {
			throw recastClientSideExceptionToServiceFailure(e);	
		}
		// send the request
		D1RestClient client = new D1RestClient(session);
		InputStream is = null;

		try {
			is = client.doPutRequest(url.getUrl(),mpe);
		} catch (BaseException be) {
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof VersionMismatch)         throw (VersionMismatch) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		return deserializeServiceType(Identifier.class, is);
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_authorization.isAuthorized */

	public boolean isAuthorized(Session session, Identifier pid, Permission permission)
	throws ServiceFailure, InvalidToken, NotFound, NotAuthorized, 
	NotImplemented, InvalidRequest
	{
		// TODO: create JavaDoc and fix doc reference
		return super.isAuthorized(session, pid, permission);
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_authorization.setAccessPolicy */

	public  boolean setAccessPolicy(Session session, Identifier pid, 
			AccessPolicy accessPolicy, long serialVersion) 
	throws InvalidToken, NotFound, NotImplemented, NotAuthorized, 
		ServiceFailure, InvalidRequest, VersionMismatch
	{

		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCESS);
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("pid", pid);
    		mpe.addFilePart("accessPolicy", accessPolicy);
    		mpe.addParamPart("serialVersion", String.valueOf(serialVersion));
    	} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(session);

		try {
			client.doPutRequest(url.getUrl(),mpe);
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

		return true;
	}

	
	//////////  CN IDENTITY API  ///////////////
	
	
	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_identity.registerAccount */

	public  Subject registerAccount(Session session, Person person) 
			throws ServiceFailure, NotAuthorized, IdentifierNotUnique, InvalidCredentials, 
			NotImplemented, InvalidRequest
	{
		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNTS);
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("person", person);
    	} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(session);

		InputStream is = null;
		try {
			is = client.doPostRequest(url.getUrl(),mpe);
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof IdentifierNotUnique)    throw (IdentifierNotUnique) be;
			if (be instanceof InvalidCredentials)     throw (InvalidCredentials) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 


		return deserializeServiceType(Subject.class, is);
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_identity.updateAccount */

	public  Subject updateAccount(Session session, Person person) 
			throws ServiceFailure, NotAuthorized, IdentifierNotUnique, InvalidCredentials, 
			NotImplemented, InvalidRequest, NotFound
	{
		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNTS);
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("person", person);
    	} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(session);

		InputStream is = null;
		try {
			is = client.doPutRequest(url.getUrl(),mpe);
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof IdentifierNotUnique)    throw (IdentifierNotUnique) be;
			if (be instanceof InvalidCredentials)     throw (InvalidCredentials) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof NotFound)               throw (NotFound) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		return deserializeServiceType(Subject.class, is);
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_identity.verifyAccount */

	public boolean verifyAccount(Session session, Subject subject) 
			throws ServiceFailure, NotAuthorized, NotImplemented, InvalidToken, 
			InvalidRequest, NotFound
	{
		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNTS);
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	url.addNextPathElement(subject.getValue());

        D1RestClient client = new D1RestClient(session);

		try {
			client.doPostRequest(url.getUrl(),mpe);
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof NotFound)               throw (NotFound) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		return true;
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_identity.getSubjectInfo */

	public SubjectInfo getSubjectInfo(Session session, Subject subject)
	throws ServiceFailure, NotAuthorized, NotImplemented, NotFound
	{
		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNTS);
    	url.addNextPathElement(subject.getValue());

		D1RestClient client = new D1RestClient(session);
		InputStream is = null;

		try {
			is = client.doGetRequest(url.getUrl());
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotFound)               throw (NotFound) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		return deserializeServiceType(SubjectInfo.class, is);
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_identity.listSubjects */

	public SubjectInfo listSubjects(Session session, String query, String status, Integer start, 
			Integer count) throws InvalidRequest, ServiceFailure, InvalidToken, NotAuthorized, 
			NotImplemented
	{
		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNTS);
    	url.addNonEmptyParamPair("query", query);
    	url.addNonEmptyParamPair("status", status);
    	url.addNonEmptyParamPair("start", start);
    	url.addNonEmptyParamPair("count", count);
    	
		D1RestClient client = new D1RestClient(session);
		InputStream is = null;

		try {
			is = client.doGetRequest(url.getUrl());
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
		
		return deserializeServiceType(SubjectInfo.class, is);
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_identity.mapIdentity */

	public boolean mapIdentity(Session session, Subject primarySubject, Subject secondarySubject) 
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented, InvalidRequest, IdentifierNotUnique
	{
		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING);
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("primarySubject", primarySubject);
    		mpe.addFilePart("secondarySubject", secondarySubject);
    	} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(session);

		try {
			client.doPostRequest(url.getUrl(),mpe);
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

		return true;
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_identity.requestMapIdentity */

	public boolean requestMapIdentity(Session session, Subject subject) 
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented, InvalidRequest
	{
		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING_PENDING);
    	url.addNextPathElement(subject.getValue());
    	
		D1RestClient client = new D1RestClient(session);

		try {
			client.doPostRequest(url.getUrl(),null);
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

		return true;
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_identity.confirmMapIdentity */

	public boolean confirmMapIdentity(Session session, Subject subject) 
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented
	{

		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING_PENDING);
    	url.addNextPathElement(subject.getValue());

		D1RestClient client = new D1RestClient(session);

		try {
			client.doPutRequest(url.getUrl(), null);
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

		return true;
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_identity.denyMapIdentity */

	public boolean denyMapIdentity(Session session, Subject subject) 
	throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented, InvalidRequest
	{
		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING_PENDING);
    	url.addNextPathElement(subject.getValue());
		D1RestClient client = new D1RestClient(session);
		try {
			client.doDeleteRequest(url.getUrl());
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

		return true;
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_identity.removeMapIdentity */

	public  boolean removeMapIdentity(Session session, Subject subject) 
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound, 
			NotImplemented, InvalidRequest
	{
		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCOUNT_MAPPING);
    	url.addNextPathElement(subject.getValue());
		D1RestClient client = new D1RestClient(session);

		try {
			client.doDeleteRequest(url.getUrl());
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

		return true;
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_identity.createGroup */

	public Subject createGroup(Session session, Subject groupName) 
	throws ServiceFailure, InvalidToken, NotAuthorized, NotImplemented, IdentifierNotUnique
	{
		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_GROUPS);
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	url.addNextPathElement(groupName.getValue());
		// send the request
		D1RestClient client = new D1RestClient(session);
		InputStream is = null;

		try {
			is = client.doPostRequest(url.getUrl(),mpe);
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

		return deserializeServiceType(Subject.class, is);
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_identity.addGroupMembers */

	public  boolean addGroupMembers(Session session, Subject groupName, 
			SubjectList members) throws ServiceFailure, InvalidToken, 
			NotAuthorized, NotFound, NotImplemented, InvalidRequest
	{
		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_GROUPS);
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
        	url.addNextPathElement(groupName.getValue());
    		//mpe.addFilePart("groupName", groupName);
    		mpe.addFilePart("members", members);
    	} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(session);

		try {
			client.doPutRequest(url.getUrl(),mpe);
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

		return true;
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_identity.removeGroupMembers */

	public boolean removeGroupMembers(Session session, Subject groupName, 
			SubjectList members) throws ServiceFailure, InvalidToken, NotAuthorized, 
			NotFound, NotImplemented, InvalidRequest
	{

		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_GROUPS_REMOVE);
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		url.addNextPathElement(groupName.getValue());
    		//mpe.addFilePart("groupName", groupName);
    		mpe.addFilePart("members", members);
    	} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(session);

		try {
			client.doPostRequest(url.getUrl(),mpe);
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

		return true;
	}



	///////////// CN REGISTER API   ////////////////

	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_register.updateNodeCapabilities */

	public boolean updateNodeCapabilities(Session session, NodeReference nodeid, 
	Node node) throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, NotFound
	{

		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_GROUPS);
    	url.addNextPathElement(nodeid.getValue());
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("node", node);
    	} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(session);

		try {
			client.doPutRequest(url.getUrl(),mpe);
		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof NotFound)               throw (NotFound) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		return true;
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_register.register */

	public NodeReference register(Session session, Node node)
	throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, 
	IdentifierNotUnique
	{
		// TODO: create JavaDoc and fix doc reference

    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_NODE);
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("node", node);
    	} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(session);

		InputStream is = null;
		try {
			is = client.doPostRequest(url.getUrl(),mpe);
		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof IdentifierNotUnique)    throw (IdentifierNotUnique) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		return deserializeServiceType(NodeReference.class, is);
	}


	////////////  CN REPLICATION API    ////////////////////
	
	
	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_replication.setReplicationStatus */

	public  boolean setReplicationStatus(Session session, Identifier pid, 
			NodeReference nodeRef, ReplicationStatus status, long serialVersion) 
					throws ServiceFailure, NotImplemented, InvalidToken, NotAuthorized, 
					InvalidRequest, NotFound, VersionMismatch
	{
		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_REPLICATION_NOTIFY);
    	url.addNextPathElement(pid.getValue());
    	
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addFilePart("status", status);
    		mpe.addFilePart("nodeRef", nodeRef);
    		mpe.addParamPart("serialVersion", String.valueOf(serialVersion));
    	} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(session);

		try {
			client.doPostRequest(url.getUrl(),mpe);
		} catch (BaseException be) {
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof VersionMismatch)         throw (VersionMismatch) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		return true;
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_replication.setReplicationPolicy */

	public boolean setReplicationPolicy(Session session, Identifier pid, 
			ReplicationPolicy policy, long serialVersion) 
					throws NotImplemented, NotFound, NotAuthorized, ServiceFailure, 
					InvalidRequest, InvalidToken, VersionMismatch
	{

		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_REPLICATION_POLICY);
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

		D1RestClient client = new D1RestClient(session);

		try {
			client.doPutRequest(url.getUrl(),mpe);
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

		return true;
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_replication.isNodeAuthorized */

	public  boolean isNodeAuthorized(Session originatingNodeSession, 
			Subject targetNodeSubject, Identifier pid, Permission replicatePermission)
	throws NotImplemented, NotAuthorized, InvalidToken, ServiceFailure, 
	NotFound, InvalidRequest
	{

		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_REPLICATION_AUTHORIZED);
        url.addNextPathElement(pid.getValue());
        
        url.addNonEmptyParamPair("targetNodeSubject", targetNodeSubject.getValue());
        url.addNonEmptyParamPair("replicatePermission", Permission.REPLICATE.toString());
        D1RestClient client = new D1RestClient(originatingNodeSession);

		try {
			client.doGetRequest(url.getUrl());
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

		return true;
	}


	/* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CN_replication.updateReplicationMetadata */

	public boolean updateReplicationMetadata(Session targetNodeSession, 
			Identifier pid, Replica replicaMetadata, long serialVersion)
	throws NotImplemented, NotAuthorized, ServiceFailure, NotFound, 
	InvalidRequest, VersionMismatch
	{
		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_REPLICATION_META);
		url.addNextPathElement(pid.getValue());

		SimpleMultipartEntity mpe = new SimpleMultipartEntity();

		try {
			mpe.addFilePart("replica", replicaMetadata);
			mpe.addParamPart("serialVersion", String.valueOf(serialVersion));
		} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

		D1RestClient client = new D1RestClient(targetNodeSession);

		try {
			client.doPutRequest(url.getUrl(),mpe);
		} catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotFound)               throw (NotFound) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof VersionMismatch)         throw (VersionMismatch) be;

			throw recastDataONEExceptionToServiceFailure(be);
		} 
		catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
		catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
		catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

		return true;
	}
}
