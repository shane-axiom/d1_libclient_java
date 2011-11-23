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
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.client.ClientProtocolException;
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
import org.dataone.service.mn.tier1.v1.MNCore;
import org.dataone.service.mn.tier1.v1.MNRead;
import org.dataone.service.mn.tier2.v1.MNAuthorization;
import org.dataone.service.mn.tier3.v1.MNStorage;
import org.dataone.service.mn.tier4.v1.MNReplication;
import org.dataone.service.types.v1.AccessPolicy;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.DescribeResponse;
import org.dataone.service.types.v1.Event;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Log;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.BigIntegerMarshaller;
import org.dataone.service.util.Constants;
import org.dataone.service.util.D1Url;
import org.dataone.service.util.DateTimeMarshaller;
import org.dataone.service.util.EncodingUtilities;
import org.jibx.runtime.JiBXException;

/**
 * MNode represents a MemberNode, and exposes the services associated with a
 * DataONE Member Node, allowing calling clients to call the services associated
 * with the node.
 * 
 * By necessity, behavior for null values differs slightly from the REST api. 
 * Except where noted, null values in method parameters will:
 * 1. throw a NotFound or InvalidRequest exception if the parameter is mapped to the URL path segment
 * 2. omit the url query parameter if the parameter is mapped to a URL query segment
 * 3. throw an InvalidRequest exception if the parameter is mapped to the message body
 * 
 * Session certificates will not be passed to the httpClient request, as they are
 * pulled from the underlying filesystem of the local machine.
 * 
 * Java implementation of the following types:
 * Types.OctectStream - java.io.InputStream
 * unsignedlong - we use long to keep to a native type, although only half capacity of unsigned
 * Types.DateTime - java.util.Date
 * 
 *  @author Rob Nahf
 */

public class MNode extends D1Node 
implements MNCore, MNRead, MNAuthorization, MNStorage, MNReplication 
{
  
	protected static org.apache.commons.logging.Log log = LogFactory.getLog(MNode.class);
	
    /**
     * Construct a new client-side MNode (Member Node) object, 
     * passing in the base url of the member node for calling its services.
     * @param nodeBaseServiceUrl base url for constructing service endpoints.
     */
    public MNode(String nodeBaseServiceUrl) {
        super(nodeBaseServiceUrl); 
    }
   
    
    public String getNodeBaseServiceUrl() {
    	D1Url url = new D1Url(super.getNodeBaseServiceUrl());
    	url.addNextPathElement(MNCore.SERVICE_VERSION);
    	log.debug("Node base service URL is: " + url.getUrl());
    	return url.getUrl();
    }
    
    /* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_core.ping */

    public boolean ping() 
    throws NotImplemented, ServiceFailure, InsufficientResources
    {

    	// TODO: create JavaDoc and fix doc reference

    	// assemble the url
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_MONITOR_PING);
    	// send the request
    	D1RestClient client = new D1RestClient();

    	try {
    		client.doGetRequest(url.getUrl());
    	} catch (BaseException be) {
    		if (be instanceof NotImplemented)         throw (NotImplemented) be;
    		if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
    		if (be instanceof InsufficientResources)  throw (InsufficientResources) be;

    		throw recastDataONEExceptionToServiceFailure(be);
    	} 
    	catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
    	catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
    	catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
    	catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

    	// if exception not thrown, and we got this far,
    	// then success (input stream should be empty)
    	return true;
    }


    /* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_core.getLogRecords */

    public Log getLogRecords(Session session, Date fromDate, Date toDate, 
               Event event, Integer start, Integer count) 
    throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure
    {
        // TODO: create JavaDoc and fix doc reference

    	Log theLog = null;
		try {
			theLog = super.getLogRecords(session, fromDate, toDate, event, start, count);
		} catch (InsufficientResources e) {
			recastDataONEExceptionToServiceFailure(e);
		}
    	return theLog;
    }

    
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

    /* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_core.getCapabilities */

    public Node getCapabilities() 
    throws NotImplemented, ServiceFailure
    {
        // TODO: create JavaDoc and fix doc reference

    	// assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_NODE);

        // send the request
        D1RestClient client = new D1RestClient();
        InputStream is = null;

        try {
        	is = client.doGetRequest(url.getUrl());
        } catch (BaseException be) {
            if (be instanceof NotImplemented)    throw (NotImplemented) be;
            if (be instanceof ServiceFailure)    throw (ServiceFailure) be;
                    
            throw recastDataONEExceptionToServiceFailure(be);
        } 
        catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
        catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

        return deserializeServiceType(Node.class, is);
    }


    /* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.get */

    public InputStream get(Session session, Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
        // TODO: create JavaDoc and fix doc reference
    	return super.get(session, pid);
    }


    /* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.getSystemMetadata */

    public SystemMetadata getSystemMetadata(Session session, Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
        // TODO: create JavaDoc and fix doc reference
    	return super.getSystemMetadata(session,pid);
    }


    /* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.describe */

    public DescribeResponse describe(Session session, Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
        // TODO: create JavaDoc and fix doc reference

    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
    	
    	if(pid == null || pid.getValue().trim().equals(""))
    		throw new NotFound("0000", "supplied PID was null, and cannot be");
    	url.addNextPathElement(pid.getValue());
    	
     	D1RestClient client = new D1RestClient(session);
    	
    	Header[] h = null;
    	Map<String, String> headersMap = new HashMap<String,String>();
    	try {
    		h = client.doHeadRequest(url.getUrl());
    		for (int i=0;i<h.length; i++) {
    			log.debug("header: " + h[i].getName() +
    					" = " + h[i].getValue());
    			headersMap.put(h[i].getName(),h[i].getValue());
    		}
        } catch (BaseException be) {
            if (be instanceof InvalidToken)     throw (InvalidToken) be;
            if (be instanceof NotAuthorized)    throw (NotAuthorized) be;
            if (be instanceof NotImplemented)   throw (NotImplemented) be;
            if (be instanceof ServiceFailure)   throw (ServiceFailure) be;
            if (be instanceof NotFound)         throw (NotFound) be;
                    
            throw recastDataONEExceptionToServiceFailure(be);
        } 
        catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
        catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

 //   	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        String objectFormatIdStr = headersMap.get("DataONE-ObjectFormat");//.get(0);
        String last_modifiedStr = headersMap.get("Last-Modified");//.get(0);
        String content_lengthStr = headersMap.get("Content-Length");//.get(0);
        String checksumStr = headersMap.get("DataONE-Checksum");//.get(0);
   
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
        
        ObjectFormatIdentifier ofID = new ObjectFormatIdentifier();
        ofID.setValue(objectFormatIdStr);

        return new DescribeResponse(ofID, content_length, last_modified, checksum);
    }


    /* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.getChecksum */

    public Checksum getChecksum(Session session, Identifier pid, String checksumAlgorithm)
    throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {

        // TODO: create JavaDoc and fix doc reference
    	
    	if(pid == null || pid.getValue().trim().equals(""))
            throw new InvalidRequest("1402", "GUID cannot be null nor empty");

        // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_CHECKSUM);
    	url.addNextPathElement(pid.getValue());
        
    	url.addNonEmptyParamPair("checksumAlgorithm", checksumAlgorithm);

        // send the request
        D1RestClient client = new D1RestClient(session);
        InputStream is = null;

        try {
        	is = client.doGetRequest(url.getUrl());
        } catch (BaseException be) {
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof NotFound)               throw (NotFound) be;
                    
            throw recastDataONEExceptionToServiceFailure(be);
        } 
        catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
        catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

        return deserializeServiceType(Checksum.class, is);
    }

    


    /* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.synchronizationFailed */

    public void synchronizationFailed(Session session, SynchronizationFailed message)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure
    {

        // TODO: create JavaDoc and fix doc reference
    	
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ERROR);
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
			mpe.addFilePart("message", message.serialize(SynchronizationFailed.FMT_XML));
		} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}
    	
        // send the request
        D1RestClient client = new D1RestClient(session);

        try {
        	client.doPostRequest(url.getUrl(),mpe);
        } catch (BaseException be) {
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
                    
            throw recastDataONEExceptionToServiceFailure(be);
        } 
        catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
        catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 
        
        // no return
    }


    /* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_authorization.isAuthorized */

    public boolean isAuthorized(Session session, Identifier pid, Permission action)
    throws ServiceFailure, InvalidRequest, InvalidToken, NotFound, NotAuthorized, NotImplemented
    {
    	// TODO: create JavaDoc and fix doc reference
    	return super.isAuthorized(session, pid, action);
    }



    /* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_authorization.setAccessPolicy */
    @Deprecated
    public boolean setAccessPolicy(Session session, Identifier pid, AccessPolicy accessPolicy)
    throws InvalidToken, ServiceFailure, NotFound, NotAuthorized, 
      NotImplemented, InvalidRequest
    {

        // TODO: create JavaDoc and fix doc reference

    	if(pid == null || pid.getValue().trim().equals(""))
            throw new InvalidRequest("1402", "GUID cannot be null nor empty");

        // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ACCESS);
    	url.addNextPathElement(pid.getValue());
    	
    	// put the accessPolicy in the message body
        SimpleMultipartEntity smpe = new SimpleMultipartEntity();
        try {
			smpe.addFilePart("accessPolicy", accessPolicy);
		} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}

    	// send the request
    	D1RestClient client = new D1RestClient(session);
    	try {
    		client.doPutRequest(url.getUrl(),smpe);
        } catch (BaseException be) {
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof NotFound)               throw (NotFound) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
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


    /* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_storage.create */

    public  Identifier create(Session session, Identifier pid, InputStream object, 
            SystemMetadata sysmeta) 
    throws IdentifierNotUnique, InsufficientResources, InvalidRequest, InvalidSystemMetadata, 
        	InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, UnsupportedType
    {

        // TODO: create JavaDoc and fix doc reference

    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
    	url.addNextPathElement(pid.getValue());

    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
			mpe.addFilePart("object",object);
			mpe.addFilePart("sysmeta", sysmeta);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (JiBXException e) {
			throw recastClientSideExceptionToServiceFailure(e);	
		}

    	
    	D1RestClient client = new D1RestClient(session);
    	InputStream is = null;

    	try {
    		is = client.doPostRequest(url.getUrl(),mpe);
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
                    
            throw recastDataONEExceptionToServiceFailure(be);
        } 
        catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
        catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

        return deserializeServiceType(Identifier.class, is);
    }


    /* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_storage.update */

    public  Identifier update(Session session, Identifier pid, InputStream object, 
            Identifier newPid, SystemMetadata sysmeta) 
        throws IdentifierNotUnique, InsufficientResources, InvalidRequest, InvalidSystemMetadata, 
            InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, UnsupportedType,
            NotFound
    {
        // TODO: create JavaDoc and fix doc reference
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
    	url.addNextPathElement(pid.getValue());

    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	mpe.addParamPart("newPid", 
                EncodingUtilities.encodeUrlQuerySegment(newPid.getValue()));
    	try {
			mpe.addFilePart("object",object);
			mpe.addFilePart("sysmeta", sysmeta);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (JiBXException e) {
			throw recastClientSideExceptionToServiceFailure(e);	
		}
    	  	
    	D1RestClient client = new D1RestClient(session);
    	InputStream is = null;
    	
    	try {
    		is = client.doPutRequest(url.getUrl(),mpe);
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
                    
            throw recastDataONEExceptionToServiceFailure(be);
        } 
        catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
        catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

        return deserializeServiceType(Identifier.class, is);
    }


    /* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_storage.delete */

    public  Identifier delete(Session session, Identifier pid)
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
    {

        // TODO: create JavaDoc and fix doc reference

    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
    	
    	if(pid == null || pid.getValue().trim().equals(""))
    		throw new NotFound("0000", "GUID cannot be null.");
    	url.addNextPathElement(pid.getValue());
    	
     	D1RestClient client = new D1RestClient(session);
    	
    	InputStream is = null;
    	try {
    		is = client.doDeleteRequest(url.getUrl());
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

        return deserializeServiceType(Identifier.class, is);
    }


    /* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_storage.systemMetadataChanged */

    public  void systemMetadataChanged(Session session, Identifier pid, long serialVersion,
        	Date dateSystemMetadataLastModified)
        throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest
    {

        // TODO: create JavaDoc and fix doc reference

    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_META_CHANGED);

		SimpleMultipartEntity mpe = new SimpleMultipartEntity();
        mpe.addParamPart("pid", pid.getValue());
        mpe.addParamPart("dateSysMetaLastModified", 
                DateTimeMarshaller.serializeDateToUTC(dateSystemMetadataLastModified));
        mpe.addParamPart("serialVersion", String.valueOf(serialVersion));
		    	
		D1RestClient client = new D1RestClient(session);
		try {
			client.doPostRequest(url.getUrl(), mpe);
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
    }


    /* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_replication.replicate */

    public boolean replicate(Session session, SystemMetadata sysmeta, NodeReference sourceNode) 
    throws NotImplemented, ServiceFailure, NotAuthorized, InvalidRequest, 
        InsufficientResources, UnsupportedType
    {
        // TODO: create JavaDoc and fix doc reference

    	if (sysmeta == null)
    		throw new InvalidRequest("2153","'sysmeta' cannot be null");
		if (sourceNode == null)
    		throw new InvalidRequest("2153","'sourceNode' cannot be null");
		
		// assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_REPLICATE);
    	
    	// assemble the context body
    	SimpleMultipartEntity smpe = new SimpleMultipartEntity();
    	smpe.addParamPart("sourceNode", sourceNode.getValue());
    	try {
			smpe.addFilePart("sysmeta", sysmeta);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (JiBXException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		}
    	
    	D1RestClient client = new D1RestClient(session);
    	try {
			client.doPostRequest(url.getUrl(),smpe);
        } catch (BaseException be) {
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
            if (be instanceof InsufficientResources)  throw (InsufficientResources) be;
            if (be instanceof UnsupportedType)        throw (UnsupportedType) be;
                    
            throw recastDataONEExceptionToServiceFailure(be);
        } 
        catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
        catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

        return true;
    }


    /* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_replication.getReplica */

    public InputStream getReplica(Session session, Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
        // TODO: create JavaDoc and fix doc reference

        D1Url url = new D1Url(this.getNodeBaseServiceUrl(),Constants.RESOURCE_REPLICAS);
        url.addNextPathElement(pid.getValue());

        // send the request
        D1RestClient client = new D1RestClient(session);
        InputStream is = null;
        try {
        	is = client.doGetRequest(url.getUrl());
        } catch (BaseException be) {
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof NotFound)               throw (NotFound) be;
                    
            throw recastDataONEExceptionToServiceFailure(be);
        } 
        catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
        catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

        return is;
    }
}
