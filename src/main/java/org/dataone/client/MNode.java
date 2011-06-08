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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.dataone.mimemultipart.MultipartRequestHandler;
import org.dataone.mimemultipart.SimpleMultipartEntity;
import org.dataone.service.Constants;
import org.dataone.service.D1Url;
import org.dataone.service.EncodingUtilities;
import org.dataone.service.exceptions.AuthenticationTimeout;
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
import org.dataone.service.exceptions.UnsupportedQueryType;
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.mn.MemberNodeCrud;
import org.dataone.service.mn.MemberNodeReplication;
import org.dataone.service.mn.tier1.MNCore;
import org.dataone.service.mn.tier1.MNRead;
import org.dataone.service.mn.tier2.MNAuthorization;
import org.dataone.service.mn.tier3.MNStorage;
import org.dataone.service.mn.tier4.MNReplication;
import org.dataone.service.types.AccessPolicy;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Checksum;
import org.dataone.service.types.ChecksumAlgorithm;
import org.dataone.service.types.DescribeResponse;
import org.dataone.service.types.Event;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.Log;
import org.dataone.service.types.MonitorList;
import org.dataone.service.types.Node;
import org.dataone.service.types.NodeReference;
import org.dataone.service.types.ObjectFormat;
import org.dataone.service.types.ObjectList;
import org.dataone.service.types.Permission;
import org.dataone.service.types.Session;
import org.dataone.service.types.Subject;
import org.dataone.service.types.SystemMetadata;
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
     * Construct a Member Node, passing in the base url for node services.
     * @param nodeBaseServiceUrl base url for constructing service endpoints.
     */
    public MNode(String nodeBaseServiceUrl) {
        super(nodeBaseServiceUrl);
        // Look up the node from the CN Node list
        CNode cn = D1Client.getCN();
        String regNodeId = cn.lookupNodeId(nodeBaseServiceUrl);
        this.setNodeId(regNodeId);
    }
    
    /**
     * Construct a Member Node, passing in the identifier for the node, from which the
     * node's base url is looked up at the coordinating node.
     * @param nodeRef the reference to the Node's identifier
     */
    public MNode(NodeReference nodeRef) {
        this.setNodeId(nodeRef.getValue());
        // Look up the node from the CN Node list
        CNode cn = D1Client.getCN();
        String baseUrl = cn.lookupNodeBaseUrl(nodeRef.getValue());
        setNodeBaseServiceUrl(baseUrl);
    }
    
    ////////////////   Tier 1 :  MNCore API   //////////////////////
    
    /**
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_core.ping
     */
	@Override
	public boolean ping() 
	throws NotImplemented, ServiceFailure, NotAuthorized,
		InvalidRequest, InsufficientResources, UnsupportedType 
	{
		 // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_MONITOR_PING);

    	// send the request
    	D1RestClient client = new D1RestClient(true, verbose);
    	InputStream is = null;
    	
    	try {
			is = client.doGetRequest(url.getUrl());
    	} catch (NotFound e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (UnsupportedQueryType e) {
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
    	} catch (UnsupportedType e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		}   catch (ClientProtocolException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IllegalStateException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IOException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (HttpException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
		}
    	// if exception not thrown, and we got this far,
    	// then success (input stream should be empty)
    	return true;
	}

	/** 
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_core.getLogRecords
     */
	@Override
	public Log getLogRecords(Session cert, Date fromDate, Date toDate,
			Event event, Integer start, Integer count) throws InvalidToken,
			ServiceFailure, NotAuthorized, InvalidRequest, NotImplemented {
		 // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_LOG);
        
//    	if (token != null)
//    		url.addNonEmptyParamPair("sessionid", token.getToken());
    	url.addDateParamPair("fromDate", fromDate);
    	url.addDateParamPair("toDate", toDate);
    	url.addNonEmptyParamPair("event", event.toString());

    	// send the request
    	D1RestClient client = new D1RestClient(true, verbose);
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
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IllegalStateException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IOException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (HttpException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	}  

        try {
            return (Log) deserializeServiceType(Log.class, is);
        } catch (Exception e) {
            throw new ServiceFailure("1090", "Could not deserialize the Log: "
                    + e.getMessage());
        }

	}

	/**
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_core.getOperationStatistics
     */
	@Override
	public MonitorList getOperationStatistics(Session cert, Integer period,
		Subject requestor, Event event, ObjectFormat format)
	throws NotImplemented, ServiceFailure, NotAuthorized,
		InvalidRequest, InsufficientResources, UnsupportedType 
	{
		
		 // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_MONITOR_EVENT);
//        url.addNextPathElement("event");
        
//    	if (token != null)
//    		url.addNonEmptyParamPair("sessionid", token.getToken());
        url.addNonEmptyParamPair("period", period);
    	url.addNonEmptyParamPair("requrestor", requestor.getValue());
    	url.addNonEmptyParamPair("event", event.toString());

    	// send the request
    	D1RestClient client = new D1RestClient(true, verbose);
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
    	} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (UnsupportedMetadataType e) {
    		throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
    	} catch (ClientProtocolException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IllegalStateException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IOException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (HttpException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
		}  

    	try {
    		return (MonitorList) deserializeServiceType(MonitorList.class, is);
    	} catch (Exception e) {
    		throw new ServiceFailure("1090", "Could not deserialize the MonitorList: "
    				+ e.getMessage());
    	}
	}

	 /**
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_core.getCapabilities
     */
	@Override
	public Node getCapabilities() 
	throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest
	{
		 // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_NODE);

    	// send the request
    	D1RestClient client = new D1RestClient(true, verbose);
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
		}   catch (ClientProtocolException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IllegalStateException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IOException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (HttpException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
		}

    	try {
    		return (Node) deserializeServiceType(Node.class, is);
    	} catch (Exception e) {
    		throw new ServiceFailure("1090", "Could not deserialize the Node: "
    				+ e.getMessage());
    	}	
	}

	
	/////////////////////    Tier 1 :  MNRead API    //////////////////////
	
	/**
     * InputStream is the Java native version of D1's OctetStream
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.listObjects
     *
     */
    public InputStream get(Session cert, Identifier pid)
    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, 
    NotImplemented, InvalidRequest 
    {
       	D1Url url = new D1Url(this.getNodeBaseServiceUrl(),Constants.RESOURCE_OBJECTS);
    	url.addNextPathElement(pid.getValue());
//       	if (cert != null)
//    		url.addNonEmptyParamPair("sessionid",token.getToken());

		D1RestClient client = new D1RestClient(true, verbose);
		
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
		} catch (UnsupportedMetadataType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidRequest e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (AuthenticationTimeout e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IllegalStateException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (HttpException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		}
		return is;
    }
    
    
	/**
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.getSystemMetadata
     */
	@Override
	public SystemMetadata getSystemMetadata(Session cert, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
		InvalidRequest, NotImplemented 
	{
		
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(),Constants.RESOURCE_META);
    	url.addNextPathElement(pid.getValue());
//       	if (cert != null)
//    		url.addNonEmptyParamPair("sessionid",cert.);

		D1RestClient client = new D1RestClient(true, verbose);
		
		InputStream is = null;
	
		try {
			is = client.doGetRequest(url.getUrl());		
		} catch (IdentifierNotUnique e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedType e) {
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
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IllegalStateException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (HttpException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		}
		try {
            return deserializeSystemMetadata(is);
        } catch (Exception e) {
            throw new ServiceFailure("1090",
                    "Could not deserialize the systemMetadata: " + e.getMessage());
        }	
	}

	/**
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.describe
     */
	@Override
	public DescribeResponse describe(Session cert, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
		NotImplemented, InvalidRequest 
	{
		
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
    	
    	if(pid == null || pid.getValue().trim().equals(""))
    		throw new InvalidRequest("1362", "GUID cannot be null.");
    	url.addNextPathElement(pid.getValue());
    	
//    	if (token == null)
//    		token = new AuthToken("public");
//    	url.addNonEmptyParamPair("sessionid", token.getToken());
    	
     	D1RestClient client = new D1RestClient(true, verbose);
//    	client.setHeader("token", token.getToken());
    	
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
        String formatStr = m.get("format");//.get(0);
        String last_modifiedStr = m.get("last_modified");//.get(0);
        String content_lengthStr = m.get("content_length");//.get(0);
        String checksumStr = m.get("checksum");//.get(0);
        String checksum_algorithmStr = m.get("checksum_algorithm");//.get(0);
        ObjectFormat format = ObjectFormatCache.getFormat(formatStr);
        long content_length = new Long(content_lengthStr).longValue();
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
            throw new InvalidRequest("1362", "Could not parse the date string " + 
                    last_modifiedStr + ". It should be in the format 'yyyy-MM-dd'T'hh:mm:ss.SZ': " +
                    pe.getMessage());
        }
        Checksum checksum = new Checksum();
        checksum.setAlgorithm(ChecksumAlgorithm.convert(checksum_algorithmStr));
        checksum.setValue(checksumStr);
        
        DescribeResponse dr = new DescribeResponse(format, content_length, last_modified, checksum);
        return dr;
		
	}

	
	/**
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.getChecksum
     */
	@Override
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
        
//    	if (token != null)
//    		url.addNonEmptyParamPair("sessionid", token.getToken());
    	url.addNonEmptyParamPair("checksumAlgorithm", checksumAlgorithm);

    	// send the request
    	D1RestClient client = new D1RestClient(true, verbose);
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
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IllegalStateException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IOException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (HttpException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	}  

    	try {
    		return (Checksum) deserializeServiceType(Checksum.class, is);
    	} catch (JiBXException e) {
    		throw new ServiceFailure("500",
    				"Could not deserialize the returned Checksum: " + e.getMessage());
    	}
	}

	public ObjectList listObjects(Session cert, Date startTime,
			Date endTime, ObjectFormat objectFormat, Boolean replicaStatus,
			Integer start, Integer count) 
	throws NotAuthorized, InvalidRequest, NotImplemented, ServiceFailure, InvalidToken 
	{
		if (replicaStatus == null) {
			return listObjects(cert,startTime,endTime, objectFormat, true, start, count);
		} else {
			return listObjects(cert,startTime,endTime, objectFormat, replicaStatus, start, count);
		}
	}
	
	
	/** 
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.listObjects
     */
//	@Override
	public ObjectList listObjects(Session cert, Date startTime,
			Date endTime, ObjectFormat objectFormat, boolean replicaStatus,
			Integer start, Integer count) 
	throws NotAuthorized, InvalidRequest, NotImplemented, ServiceFailure, InvalidToken 
	{
		
		if (endTime != null && startTime != null && !endTime.after(startTime))
			throw new InvalidRequest("1000", "startTime must be after stopTime in NMode.listObjects");

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
		
		url.addDateParamPair("startTime", startTime);
		url.addDateParamPair("endTime", endTime);
		if (objectFormat != null) 
			url.addNonEmptyParamPair("objectFormat", objectFormat.toString());
		if (replicaStatus) {
			url.addNonEmptyParamPair("replicaStatus", 1);
		} else {
			url.addNonEmptyParamPair("replicaStatus", 0);
		}
		url.addNonEmptyParamPair("start",start);
		url.addNonEmptyParamPair("count",count);
//    	if (token != null)
//    		url.addNonEmptyParamPair("sessionid",token.getToken());

		D1RestClient client = new D1RestClient(true, verbose);

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
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IllegalStateException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (HttpException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		
		}
         
        try {
            return deserializeObjectList(is);
        } catch (JiBXException e) {
            throw new ServiceFailure("500",
                    "Could not deserialize the ObjectList: " + e.getMessage());
        }
	}

	
    /////////////////////    Tier 2 :  MNAuthorization API   //////////////////////

	
	@Override
	public boolean isAuthorized(Session cert, Identifier pid, Permission action)
	throws ServiceFailure, InvalidRequest, InvalidToken, NotFound, NotAuthorized, NotImplemented 
	{
		if(pid == null || pid.getValue().trim().equals(""))
            throw new InvalidRequest("1761", "PID cannot be null nor empty");

        // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_AUTHORIZATION);
    	url.addNextPathElement(pid.getValue());
    	url.addNonEmptyParamPair("action", action.toString());
    	
    	D1RestClient client = new D1RestClient(true, verbose);
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

	
	@Override
	public boolean setAccess(Session cert, Identifier pid, AccessPolicy accessPolicy) 
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
			smpe.addFilePart("accessPolicy", accessPolicy, AccessPolicy.class);
		} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		} catch (JiBXException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}
//    	if (token != null)
//    		url.addNonEmptyParamPair("sessionid", token.getToken());
//    	url.addNonEmptyParamPair("action", accessPolicy.);

    	// send the request
    	D1RestClient client = new D1RestClient(true, verbose);
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
	@Override
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
			mpe.addFilePart("sysmeta", sysmeta, SystemMetadata.class);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (JiBXException e) {
			throw recastClientSideExceptionToServiceFailure(e);	
		}

    	
    	D1RestClient client = new D1RestClient(true, verbose);
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
    	} catch (ClientProtocolException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IllegalStateException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IOException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (HttpException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	}    	
    	try {
            return (Identifier)deserializeServiceType(Identifier.class, is);
        } catch (Exception e) {
            throw new ServiceFailure("1090",
                    "Could not deserialize the systemMetadata: " + e.getMessage());
        }
	}

	
	/**
     * update a resource with the specified pid.
     */
	@Override
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
			mpe.addFilePart("sysmeta", sysmeta, SystemMetadata.class);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (JiBXException e) {
			throw recastClientSideExceptionToServiceFailure(e);	
		}
    	  	
    	D1RestClient client = new D1RestClient(true, verbose);
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
    	} catch (ClientProtocolException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IllegalStateException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IOException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (HttpException e) {
    		throw recastClientSideExceptionToServiceFailure(e);    	}    	
    	try {
    		return (Identifier)deserializeServiceType(Identifier.class, is);
    	} catch (Exception e) {
    		throw new ServiceFailure("1090",
    				"Could not deserialize the returned Identifier: " + e.getMessage());
    	}
	}

	
	/**
     * delete a resource with the specified guid. NOT IMPLEMENTED.
     */
	@Override
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
    	
     	D1RestClient client = new D1RestClient(true, verbose);
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
    	} catch (ClientProtocolException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IllegalStateException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (IOException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
    	} catch (HttpException e) {
    		throw recastClientSideExceptionToServiceFailure(e);
		}    	
    	try {
            return (Identifier)deserializeServiceType(Identifier.class, is);
        } catch (Exception e) {
            throw new ServiceFailure("1090",
                    "Could not deserialize the returned Identifier: " + e.getMessage());
        }
	}

	
    /////////////////////    Tier 4 :  MN Replicate          //////////////////////

	
	@Override
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
			smpe.addFilePart("sysmeta", sysmeta, SystemMetadata.class);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (JiBXException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		}
    	
    	D1RestClient client = new D1RestClient(true, verbose);
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

	
	
	
   //////////////   Obsolete Methods  ////////////////////////////////

    /**
     * login and get an AuthToken
     * 
     * @param username
     * @param password
     * @return
     * @throws ServiceFailure
     */
    public AuthToken login(String username, String password)
            throws ServiceFailure, NotImplemented {
        // TODO: reassess the exceptions thrown here.  Look at the Authentication interface.
        // TODO: this method assumes an access control model that is not finalized, refactor when it is

    	String postData = "username=" + username + "&password=" + password;
        String params = "qformat=xml&op=login";
        String resource = Constants.RESOURCE_SESSION + "/";

        ResponseData rd = sendRequest(null, resource, Constants.POST, params, null,
                new ByteArrayInputStream(postData.getBytes()), null);
        String sessionid = null;

        int code = rd.getCode();
        if (code != HttpURLConnection.HTTP_OK) { // deal with the error
            // TODO: detail codes are wrong, and exception is the wrong one too I think
            throw new ServiceFailure("1000", "Error logging in.");
        } else {
            try {
                InputStream is = rd.getContentStream();
                String response = IOUtils.toString(is);
                
                int successIndex = response.indexOf("<sessionId>");
                if (successIndex != -1) {
                    sessionid = response.substring(
                            response.indexOf("<sessionId>")
                                    + "<sessionId>".length(),
                            response.indexOf("</sessionId>"));
                } else {
                    // TODO: wrong exception thrown, wrong detail code?
                    throw new ServiceFailure("1000", "Error authenticating: "
                            + response.substring(response.indexOf("<error>")
                                    + "<error>".length(),
                                    response.indexOf("</error>")));
                }
            } catch (Exception e) {
                throw new ServiceFailure("1000",
                        "Error getting response from metacat: "
                                + e.getMessage());
            }
        }

        return new AuthToken(sessionid);
    }
}