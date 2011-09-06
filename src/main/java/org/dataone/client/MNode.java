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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.client.ClientProtocolException;
import org.dataone.mimemultipart.SimpleMultipartEntity;
import org.dataone.service.exceptions.AuthenticationTimeout;
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
import org.dataone.service.exceptions.SynchronizationFailed;
import org.dataone.service.exceptions.UnsupportedMetadataType;
import org.dataone.service.exceptions.UnsupportedQueryType;
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
import org.dataone.service.types.v1.MonitorList;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormat;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.BigIntegerMarshaller;
import org.dataone.service.util.Constants;
import org.dataone.service.util.D1Url;
import org.dataone.service.util.EncodingUtilities;
import org.jibx.runtime.JiBXException;

/**
 * MNode represents a MemberNode, and exposes the services associated with a
 * DataONE Member Node, allowing calling clients to call the services associated
 * with the node.
 */
public class MNode extends D1Node 
implements MNCore, MNRead, MNAuthorization, MNStorage, MNReplication 
{
    
    /**
     * Construct a new client-side MNode (Member Node) object, 
     * passing in the base url of the member node for calling its services.
     * @param nodeBaseServiceUrl base url for constructing service endpoints.
     */
    public MNode(String nodeBaseServiceUrl) {
        super(nodeBaseServiceUrl); 
    }
   
    
//    public String getNodeBaseServiceUrl() {
//    	D1Url url = new D1Url(super.getNodeBaseServiceUrl());
//    	url.addNextPathElement(MNCore.SERVICE_VERSION);
//    	return url.getUrl();
//    }
    
    ////////////////   Tier 1 :  MNCore API   //////////////////////
    
    /**
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_core.ping
     */
	public boolean ping() 
	throws 
		InvalidRequest, NotAuthorized, NotImplemented, ServiceFailure, 
		InsufficientResources, UnsupportedType 
	{
		 // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_MONITOR_PING);

    	// send the request
    	D1RestClient client = new D1RestClient();
    	InputStream is = null;
    	
    	try {
			is = client.doGetRequest(url.getUrl());
    	} 
    	catch (AuthenticationTimeout e)   { throw recastDataONEExceptionToServiceFailure(e); }
    	catch (NotFound e)                { throw recastDataONEExceptionToServiceFailure(e); }
    	catch (IdentifierNotUnique e)     { throw recastDataONEExceptionToServiceFailure(e); }
    	catch (InsufficientResources e)   { throw recastDataONEExceptionToServiceFailure(e); }
    	catch (InvalidCredentials e)      { throw recastDataONEExceptionToServiceFailure(e); }
    	catch (InvalidSystemMetadata e)   { throw recastDataONEExceptionToServiceFailure(e); }
    	catch (InvalidToken e)            { throw recastDataONEExceptionToServiceFailure(e); }
    	catch (UnsupportedMetadataType e) { throw recastDataONEExceptionToServiceFailure(e); }
    	catch (UnsupportedQueryType e)    { throw recastDataONEExceptionToServiceFailure(e); }
    	catch (UnsupportedType e)         { throw recastDataONEExceptionToServiceFailure(e); }
    	catch (SynchronizationFailed e)   { throw recastDataONEExceptionToServiceFailure(e); }
    	
		catch (ClientProtocolException e) {	throw recastClientSideExceptionToServiceFailure(e); }
    	catch (IllegalStateException e)   {	throw recastClientSideExceptionToServiceFailure(e); }
    	catch (IOException e) 		      {	throw recastClientSideExceptionToServiceFailure(e); }
    	catch (HttpException e) 		  {	throw recastClientSideExceptionToServiceFailure(e); }

    	// if exception not thrown, and we got this far,
    	// then success (input stream should be empty)
    	return true;
	}

	/** 
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_core.getLogRecords
     */
	public Log getLogRecords(Session cert, Date fromDate, Date toDate,
			Event event, Integer start, Integer count) 
	throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure
	{
		 // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_LOG);
        
    	url.addDateParamPair("fromDate", fromDate);
    	url.addDateParamPair("toDate", toDate);
    	if (event != null)
    		url.addNonEmptyParamPair("event", event.xmlValue());
    	url.addNonEmptyParamPair("start", start);
    	url.addNonEmptyParamPair("count", count);

    	// send the request
    	D1RestClient client = new D1RestClient();
    	InputStream is = null;
    	
    	try {
			is = client.doGetRequest(url.getUrl());
		} catch (NotFound e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (IdentifierNotUnique e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidSystemMetadata e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidCredentials e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (AuthenticationTimeout e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedMetadataType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (SynchronizationFailed e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IllegalStateException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IOException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (HttpException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	}  
    	return deserializeServiceType(Log.class, is);
	}

	/**
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_core.getOperationStatistics
     */
	public MonitorList getOperationStatistics(Session cert, Date startTime,
		Date endTime, Subject requestor, Event event, ObjectFormatIdentifier formatId)
	throws 
		InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure,
		InsufficientResources, UnsupportedType 
	{
		
		 // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_MONITOR_EVENT);
        
        url.addDateParamPair("startTime", startTime);
        url.addDateParamPair("endTime", endTime);
        if (requestor != null)
        	url.addNonEmptyParamPair("requestor", requestor.getValue());
        if (event != null)
        	url.addNonEmptyParamPair("event", event.xmlValue());
        if (formatId != null)
        	url.addNonEmptyParamPair("formatId", formatId.getValue());

    	// send the request
    	D1RestClient client = new D1RestClient();
    	InputStream is = null;
    	
    	try {
			is = client.doGetRequest(url.getUrl());
    	} catch (NotFound e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (IdentifierNotUnique e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (InvalidSystemMetadata e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (InvalidCredentials e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (AuthenticationTimeout e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (UnsupportedMetadataType e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (SynchronizationFailed e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
    	} catch (ClientProtocolException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IllegalStateException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IOException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (HttpException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
		}  
    	return deserializeServiceType(MonitorList.class, is);
	}

	 /**
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_core.getCapabilities
     */
	public Node getCapabilities() 
	throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest
	{
		 // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_NODE);

    	// send the request
    	D1RestClient client = new D1RestClient();
    	InputStream is = null;
    	
    	try {
			is = client.doGetRequest(url.getUrl());
    	} catch (NotFound e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (IdentifierNotUnique e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (InvalidToken e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (InvalidSystemMetadata e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (InvalidCredentials e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (AuthenticationTimeout e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (UnsupportedMetadataType e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (UnsupportedType e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (SynchronizationFailed e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		}   catch (ClientProtocolException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IllegalStateException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IOException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (HttpException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
		}
    	return deserializeServiceType(Node.class, is);
	}

	
	/////////////////////    Tier 1 :  MNRead API    //////////////////////
	
	/**
     * InputStream is the Java native version of D1's OctetStream
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.listObjects
     *
     */
	@Override
    public InputStream get(Session cert, Identifier pid)
    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, 
    NotImplemented, InvalidRequest 
    {
       	return super.get(cert, pid);
    }
    
    
	/**
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.getSystemMetadata
     */
	@Override
	public SystemMetadata getSystemMetadata(Session cert, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
		InvalidRequest, NotImplemented 
	{
		return super.getSystemMetadata(cert, pid);
	}
		

	/**
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.describe
     */
	public DescribeResponse describe(Session cert, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
		NotImplemented, InvalidRequest 
	{
		
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
    	
    	if(pid == null || pid.getValue().trim().equals(""))
    		throw new InvalidRequest("1362", "PID cannot be null.");
    	url.addNextPathElement(pid.getValue());
    	
     	D1RestClient client = new D1RestClient();
    	
    	Header[] h = null;
    	Map<String, String> m = new HashMap<String,String>();
    	try {
    		h = client.doHeadRequest(url.getUrl());
    		for (int i=0;i<h.length; i++) {
    			m.put(h[i].getName(),h[i].getValue());
    		}
    	} catch (IdentifierNotUnique e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidSystemMetadata e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidCredentials e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (AuthenticationTimeout e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedMetadataType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (SynchronizationFailed e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IllegalStateException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (HttpException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} 
    	
  
    	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        String objectFormatIdStr = m.get("DataONE-fmtid");//.get(0);
        String last_modifiedStr = m.get("Last-Modified");//.get(0);
        String content_lengthStr = m.get("Content-Length");//.get(0);
        String checksumStr = m.get("DataONE-Checksum");//.get(0);
   
        BigInteger content_length;
		try {
			content_length = BigIntegerMarshaller.deserializeBigInteger(content_lengthStr);
		} catch (JiBXException e) {
			throw new ServiceFailure("0", "Could not convert the returned content_length string (" + 
					content_lengthStr + ") to a BigInteger: " + e.getMessage());
		}
        Date last_modified = null;
        System.out.println("parsing date");
        try
        {
            last_modified = dateFormat.parse(last_modifiedStr.trim());
            /*Date d = new Date();
            dateFormat.setLenient(false);
            String dStr = dateFormat.format(d);
            System.out.println("d: " + d);
            System.out.println("dStr: " + dStr);
            Date e = dateFormat.parse(dStr);
            System.out.println("e: " + e);*/
        }
        catch(java.text.ParseException pe)
        {
            throw new ServiceFailure("0", "Could not parse the returned date string " + 
                    last_modifiedStr + ". It should be in the format 'yyyy-MM-dd'T'hh:mm:ss.SZ': " +
                    pe.getMessage());
        }

        // build a checksum object
        Checksum checksum = new Checksum();
        String[] cs = checksumStr.split(",");
        checksum.setAlgorithm(cs[0]);
        checksum.setValue(cs[1]);

        // build an objectformat identifier object
        // doesn't check validity of the formatID value
        
        // to do the check, uncomment following line, and work it in to the code
        //        ObjectFormat format = ObjectFormatCache.getInstance().getFormat(objectFormatIdStr);
        
        ObjectFormatIdentifier ofID = new ObjectFormatIdentifier();
        ofID.setValue(objectFormatIdStr);

        return new DescribeResponse(ofID, content_length, last_modified, checksum);
	}

	
	/**
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.getChecksum
     */
	public Checksum getChecksum(Session cert, Identifier pid, String checksumAlgorithm) 
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, InvalidRequest, NotImplemented 
	{
		// validate input
//    	if(token == null)
//            token = new AuthToken("public");

        if(pid == null || pid.getValue().trim().equals(""))
            throw new InvalidRequest("1402", "GUID cannot be null nor empty");

        // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_CHECKSUM);
    	url.addNextPathElement(pid.getValue());
        
    	url.addNonEmptyParamPair("checksumAlgorithm", checksumAlgorithm);

    	// send the request
    	D1RestClient client = new D1RestClient();
    	InputStream is = null;
    	
    	try {
			is = client.doGetRequest(url.getUrl());
		} catch (IdentifierNotUnique e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidSystemMetadata e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidCredentials e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (AuthenticationTimeout e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedMetadataType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (SynchronizationFailed e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IllegalStateException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IOException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (HttpException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	}  
    	return deserializeServiceType(Checksum.class, is);
	}


	   /**
     *   listObjects() is the simple implementation of /<service>/object (no query parameters, or additional path segments)
     *   use this when no parameters  being used
     *   
     * @param token
     * @return
     * @throws NotAuthorized
     * @throws InvalidRequest
     * @throws NotImplemented
     * @throws ServiceFailure
     * @throws InvalidToken
     */
    public ObjectList listObjects()
    throws NotAuthorized, InvalidRequest, NotImplemented, ServiceFailure, InvalidToken {
  
    	return listObjects(null,null,null,null,null,null,null);
        
    }
	
	
	/** 
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.listObjects
     */
	public ObjectList listObjects(Session cert, Date startTime, Date endTime, 
	ObjectFormatIdentifier objectFormatId, Boolean replicaStatus, Integer start, Integer count) 
	throws NotAuthorized, InvalidRequest, NotImplemented, ServiceFailure, InvalidToken 
	{
		
		if (endTime != null && startTime != null && !endTime.after(startTime))
			throw new InvalidRequest("1000", "startTime must be after stopTime in NMode.listObjects");

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
		
		url.addDateParamPair("startTime", startTime);
		url.addDateParamPair("endTime", endTime);
		if (objectFormatId != null) 
			url.addNonEmptyParamPair("objectFormat", objectFormatId.getValue());
		if (replicaStatus != null) {
			if (replicaStatus) {
				url.addNonEmptyParamPair("replicaStatus", 1);
			} else {
				url.addNonEmptyParamPair("replicaStatus", 0);
			}
		}
		url.addNonEmptyParamPair("start",start);
		url.addNonEmptyParamPair("count",count);

		D1RestClient client = new D1RestClient();

		InputStream is = null;
		try {
			is = client.doGetRequest(url.getUrl());
		} catch (NotFound e) {
			throw new ServiceFailure("1000", "Method threw unexpected exception: " + e.getMessage());
		} catch (IdentifierNotUnique e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidSystemMetadata e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidCredentials e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedMetadataType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (AuthenticationTimeout e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (SynchronizationFailed e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IllegalStateException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (HttpException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		}         
		return deserializeServiceType(ObjectList.class,is);      
	}

	
	@Override
	public void synchronizationFailed(Session cert, SynchronizationFailed message)
	throws InvalidToken, NotImplemented, ServiceFailure, NotAuthorized, InvalidRequest {
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ERROR);

		SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
			mpe.addFilePart("message", message.serialize(SynchronizationFailed.FMT_XML));
		} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} 
    	
		D1RestClient client = new D1RestClient();

		InputStream is = null;
		try {
			is = client.doPostRequest(url.getUrl(), mpe);
		} catch (NotFound e) {
			throw new ServiceFailure("1000", "Method threw unexpected exception: " + e.getMessage());
		} catch (IdentifierNotUnique e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidSystemMetadata e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidCredentials e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedMetadataType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (AuthenticationTimeout e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (SynchronizationFailed e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IllegalStateException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (HttpException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		}
	}
	
	
	
    /////////////////////    Tier 2 :  MNAuthorization API   //////////////////////

	
	public boolean isAuthorized(Session cert, Identifier pid, Permission action)
	throws ServiceFailure, InvalidRequest, InvalidToken, NotFound, NotAuthorized, NotImplemented 
	{
		if(pid == null || pid.getValue().trim().equals(""))
            throw new InvalidRequest("1761", "PID cannot be null nor empty");

        // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_AUTHORIZATION);
    	url.addNextPathElement(pid.getValue());
    	url.addNonEmptyParamPair("action", action.xmlValue());
    	
    	D1RestClient client = new D1RestClient();
    	InputStream is = null;
    	try {
			is = client.doGetRequest(url.getUrl());
		} catch (IdentifierNotUnique e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidSystemMetadata e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidCredentials e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (AuthenticationTimeout e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedMetadataType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (SynchronizationFailed e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IllegalStateException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IOException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (HttpException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	}  
    	return true;
	}

	
	public boolean setAccessPolicy(Session cert, Identifier pid, AccessPolicy accessPolicy) 
	throws InvalidToken, ServiceFailure, NotFound, NotAuthorized, 
	NotImplemented, InvalidRequest 
	{
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
//    	if (token != null)
//    		url.addNonEmptyParamPair("sessionid", token.getToken());
//    	url.addNonEmptyParamPair("action", accessPolicy.);

    	// send the request
    	D1RestClient client = new D1RestClient();
    	InputStream is = null;
    	
    	try {
			is = client.doPutRequest(url.getUrl(),smpe);
		} catch (IdentifierNotUnique e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidSystemMetadata e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidCredentials e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (AuthenticationTimeout e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedMetadataType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (SynchronizationFailed e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IllegalStateException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IOException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (HttpException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} 	
    	return true;
	}
	
	
    /////////////////////    Tier 3 :  MNStorage API         //////////////////////
	
    /**
     * create both a system metadata resource and science metadata resource with
     * the specified guid
     */
	public Identifier create(Session cert, Identifier pid, InputStream object,
			SystemMetadata sysmeta) 
	throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, 
	UnsupportedType, InsufficientResources, InvalidSystemMetadata, NotImplemented,
	InvalidRequest 
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
    	url.addNextPathElement(pid.getValue());
//    	if (token != null)
//    		url.addNonEmptyParamPair("sessionid", token.getToken());

    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
			mpe.addFilePart("object",object);
			mpe.addFilePart("sysmeta", sysmeta);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (JiBXException e) {
			throw recastClientSideExceptionToServiceFailure(e);	
		}

    	
    	D1RestClient client = new D1RestClient();
//    	client.setHeader("token", token.getToken());
    	InputStream is = null;

    	try {
    		is = client.doPostRequest(url.getUrl(),mpe);
    	} catch (NotFound e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (InvalidCredentials e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (InvalidRequest e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (AuthenticationTimeout e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (UnsupportedMetadataType e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (UnsupportedQueryType e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (SynchronizationFailed e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
    	} catch (ClientProtocolException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IllegalStateException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IOException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (HttpException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	}    	
    	return deserializeServiceType(Identifier.class, is);
	}

	
	/**
     * update a resource with the specified pid.
     */
	public Identifier update(Session cert, Identifier pid, InputStream object,
			Identifier newPid, SystemMetadata sysmeta) 
	throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique,
	UnsupportedType, InsufficientResources, NotFound, InvalidSystemMetadata, 
	NotImplemented, InvalidRequest 
	{
		
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
    	url.addNextPathElement(pid.getValue());
//    	if (token != null)
//    		url.addNonEmptyParamPair("sessionid", token.getToken());

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
    	  	
    	D1RestClient client = new D1RestClient();
    	InputStream is = null;
    	
    	try {
    		is = client.doPutRequest(url.getUrl(),mpe);
    	} catch (UnsupportedQueryType e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (InvalidCredentials e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (AuthenticationTimeout e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (UnsupportedMetadataType e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (SynchronizationFailed e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
    	} catch (ClientProtocolException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IllegalStateException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IOException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (HttpException e) {
    		throw recastClientSideExceptionToServiceFailure(e);    	
    	}    	
    	return deserializeServiceType(Identifier.class, is);
	}

	
	/**
     * delete a resource with the specified guid. NOT IMPLEMENTED.
     */
	public Identifier delete(Session cert, Identifier pid) 
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, 
	NotImplemented,	InvalidRequest 
	{
		
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
    	
    	if(pid == null || pid.getValue().trim().equals(""))
    		throw new InvalidRequest("1322", "GUID cannot be null.");
    	url.addNextPathElement(pid.getValue());
    	
//    	if (token == null)
//    		token = new AuthToken("public");
//    	url.addNonEmptyParamPair("sessionid", token.getToken());
    	
     	D1RestClient client = new D1RestClient();
//    	client.setHeader("token", token.getToken());
    	
    	InputStream is = null;
    	try {
    		is = client.doDeleteRequest(url.getUrl());
    	} catch (InvalidCredentials e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (AuthenticationTimeout e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (UnsupportedMetadataType e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (UnsupportedQueryType e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (IdentifierNotUnique e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (UnsupportedType e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (InsufficientResources e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (InvalidSystemMetadata e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (SynchronizationFailed e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
    	} catch (ClientProtocolException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IllegalStateException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IOException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (HttpException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
		}    	
    	return deserializeServiceType(Identifier.class, is);
	}

	
	
    /////////////////////    Tier 4 :  MN Replicate          //////////////////////

	
	public boolean replicate(Session cert, SystemMetadata sysmeta,
	NodeReference sourceNode) 
	throws NotImplemented, ServiceFailure, NotAuthorized, 
	InvalidRequest, InsufficientResources, UnsupportedType
	{
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
    	
    	D1RestClient client = new D1RestClient();
    	InputStream is = null;
    	try {
			is = client.doPostRequest(url.getUrl(),smpe);
    	} catch (IdentifierNotUnique e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidToken e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (NotFound e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidSystemMetadata e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidCredentials e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (AuthenticationTimeout e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedMetadataType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (SynchronizationFailed e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": " + e.getMessage());
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IllegalStateException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IOException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (HttpException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	}
		return true;
	}

    public InputStream getReplica(Session cert, Identifier pid)
        throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, 
        ServiceFailure, NotFound { 
       	return super.get(cert, pid);
    }


}
