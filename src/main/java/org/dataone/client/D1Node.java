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

package org.dataone.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.client.ClientProtocolException;
import org.dataone.client.cache.LocalCache;
import org.dataone.client.exception.NotCached;
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
import org.dataone.service.types.v1.Event;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Log;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.QueryEngineDescription;
import org.dataone.service.types.v1.QueryEngineList;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SystemMetadata;
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
public abstract class D1Node {

	protected static org.apache.commons.logging.Log log = LogFactory.getLog(D1Node.class);
	
    /** The URL string for the node REST API */
    private String nodeBaseServiceUrl;
    private String nodeId;
    
    /** this represents the session to be used for establishing the SSL connection */
    protected Session session;
    
    private boolean useLocalCache = false;

	private String lastRequestUrl = null;
    
    /** default Socket timeout in milliseconds **/
    private Integer defaultSoTimeout = 30000;
	/**
     * Useful for debugging to see what the last call was
     * @return
     */
    public String getLatestRequestUrl() {
    	return lastRequestUrl;
    }
    
    protected void setLatestRequestUrl(String url) {
    	lastRequestUrl = url;
    }
    
    /**
	 * Constructor to create a new instance.
	 */
	public D1Node(String nodeBaseServiceUrl, Session session) {
	    setNodeBaseServiceUrl(nodeBaseServiceUrl);
	    this.session = session;
	    // (second parameter is a default value)
	    useLocalCache = Settings.getConfiguration().getBoolean("D1Client.useLocalCache",useLocalCache);
	}
    
    
    
	/**
	 * Constructor to create a new instance.
	 */
	public D1Node(String nodeBaseServiceUrl) {
	    setNodeBaseServiceUrl(nodeBaseServiceUrl);
	    this.session = null;
	    // (second parameter is a default value)
	    useLocalCache = Settings.getConfiguration().getBoolean("D1Client.useLocalCache",useLocalCache);
	}

	// TODO: this constructor should not exist
	// lest we end up with a client that is not attached to a particular node; 
	// No code calls it in Java, but it is called by the R client; evaluate if this can change
	/**
	 * default constructor needed by some clients.  This constructor will probably
	 * go away so don't depend on it.  Use public D1Node(String nodeBaseServiceUrl) instead.
	 */
	@Deprecated
	public D1Node() {
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
    public String getNodeId() {
        return nodeId;
    }

    /**
     * @param nodeId the nodeId to set
     */
    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    
	public Date ping() throws NotImplemented, ServiceFailure, InsufficientResources 
	{
		
		// assemble the url
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_MONITOR_PING);
		
		// using the simple RestClient because we are going to get the date from
		// the headers instead of looking in the message body

		D1RestClient client = new D1RestClient();
		Header[] headers = null;
	
	    try {
	    	headers = client.doGetRequestForHeaders(url.getUrl());
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

	    finally {
	    	setLatestRequestUrl(client.getLatestRequestUrl());
	    	client.closeIdleConnections();
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
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.listObjects">see DataONE API Reference</a> }
     */
	public ObjectList listObjects() 
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure 
	{
		return listObjects(this.session);
	}
	
	
    public ObjectList listObjects(Session session) 
    throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure
    {
    	return listObjects(session,null,null,null,null,null,null);
    }

	public ObjectList listObjects(Date fromDate,
			Date toDate, ObjectFormatIdentifier formatid,
			Boolean replicaStatus, Integer start, Integer count)
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure 
	{
		return listObjects(this.session,fromDate,toDate,formatid,replicaStatus,start,count);
	}


    public ObjectList listObjects(Session session, Date fromDate, Date toDate, 
      ObjectFormatIdentifier formatid, Boolean replicaStatus, Integer start, Integer count) 
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
        D1RestClient client = new D1RestClient(session);
        client.setTimeouts(Settings.getConfiguration()
			.getInteger("D1Client.D1Node.listObjects.timeout", getDefaultSoTimeout()));
        ObjectList objectList = null;

        try {
        	InputStream is = client.doGetRequest(url.getUrl());
        	objectList =  deserializeServiceType(ObjectList.class, is);
        } catch (BaseException be) {
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
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
 
        finally {
        	setLatestRequestUrl(client.getLatestRequestUrl());
	    	client.closeIdleConnections();
	    }
        return objectList;
    }

    
    public Log getLogRecords() 
	throws InvalidToken, InvalidRequest, ServiceFailure,
	NotAuthorized, NotImplemented, InsufficientResources
	{
    	return getLogRecords(null, null, null, null, null, null);
	}   
    
    
    public Log getLogRecords(Session session) 
	throws InvalidToken, InvalidRequest, ServiceFailure,
	NotAuthorized, NotImplemented, InsufficientResources
	{
    	return getLogRecords(session, null, null, null, null, null, null);
	}
    
	
	public Log getLogRecords(Date fromDate, Date toDate,
			Event event, String pidFilter, Integer start, Integer count) 
	throws InvalidToken, InvalidRequest, ServiceFailure,
	NotAuthorized, NotImplemented, InsufficientResources
	{
		return getLogRecords(this.session, fromDate, toDate, event, pidFilter, start, count);
	}
  
	
	public Log getLogRecords(Session session, Date fromDate, Date toDate,
			Event event, String pidFilter, Integer start, Integer count) 
	throws InvalidToken, InvalidRequest, ServiceFailure,
	NotAuthorized, NotImplemented, InsufficientResources
	{

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_LOG);

		url.addDateParamPair("fromDate", fromDate);
		url.addDateParamPair("toDate", toDate);
            
    	if (event != null)
            url.addNonEmptyParamPair("event", event.xmlValue());
    	
    	url.addNonEmptyParamPair("start", start);  
    	url.addNonEmptyParamPair("count", count);
    	url.addNonEmptyParamPair("pidFilter", pidFilter);
    	
		// send the request
		D1RestClient client = new D1RestClient(session);
                client.setTimeouts(Settings.getConfiguration()
			.getInteger("D1Client.D1Node.getLogRecords.timeout", getDefaultSoTimeout()));
		Log log = null;

		try {
			InputStream is = client.doGetRequest(url.getUrl());
			log = deserializeServiceType(Log.class, is);
		} catch (BaseException be) {
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
			if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof InsufficientResources)  throw (InsufficientResources) be;

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
		return log;
	}
    
	
	/**
     * Get the resource with the specified pid.  Used by both the CNode and 
     * MNode subclasses. A LocalCache is used to cache objects in memory and in 
     * a local disk cache if the "D1Client.useLocalCache" configuration property
     * was set to true when the D1Node was created. Otherwise
     * InputStream is the Java native version of D1's OctetStream
     * 
     * @see <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.get">see DataONE API Reference (MemberNode API)</a>
     * @see <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.get">see DataONE API Reference (CoordinatingNode API)</a>
     */
	public InputStream get(Identifier pid)
    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, 
      NotImplemented, InsufficientResources
    {
		return get(this.session, pid);
    }
	
	
	/**
     * Get the resource with the specified pid.  Used by both the CNode and 
     * MNode subclasses. A LocalCache is used to cache objects in memory and in 
     * a local disk cache if the "D1Client.useLocalCache" configuration property
     * was set to true when the D1Node was created. Otherwise
     * InputStream is the Java native version of D1's OctetStream
     * 
     * @see <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.get">see DataONE API Reference (MemberNode API)</a>
     * @see <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.get">see DataONE API Reference (CoordinatingNode API)</a>
     */
    public InputStream get(Session session, Identifier pid)
    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, 
      NotImplemented, InsufficientResources
    {
        InputStream is = null;        
        boolean cacheMissed = false;
        
        if (useLocalCache) {
            try {
                byte[] data = LocalCache.instance().getData(pid);
                is = new ByteArrayInputStream(data);
                return is;
            } catch (NotCached e) {
                cacheMissed = true;
            }
        }
       	D1Url url = new D1Url(this.getNodeBaseServiceUrl(),Constants.RESOURCE_OBJECTS);
       	
       	try {
       		url.addNextPathElement(pid.getValue());
       	} catch (IllegalArgumentException e) {
       		throw new NotFound("0000", "'pid' cannot be null nor empty");
       	}
       	

		D1RestClient client = new D1RestClient(session);
                client.setTimeouts(Settings.getConfiguration()
                .getInteger("D1Client.D1Node.get.timeout", getDefaultSoTimeout()));
        try {
        	byte[] bytes = IOUtils.toByteArray(client.doGetRequest(url.getUrl()));
        	is = new ByteArrayInputStream(bytes); 
		
			if (cacheMissed) {
			    // Cache the result, and reset the stream mark
			    LocalCache.instance().putData(pid, bytes);
			}
		} catch (BaseException be) {
            if (be instanceof InvalidToken)      throw (InvalidToken) be;
            if (be instanceof NotAuthorized)     throw (NotAuthorized) be;
            if (be instanceof NotImplemented)    throw (NotImplemented) be;
            if (be instanceof ServiceFailure)    throw (ServiceFailure) be;
            if (be instanceof NotFound)                throw (NotFound) be;
            if (be instanceof InsufficientResources)   throw (InsufficientResources) be;
                    
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
        return is;
    }
 
    
    
    /**
     * Get the system metadata from a resource with the specified guid. Used
     * by both the CNode and MNode implementations. Note that this method defaults
     * to not using the local system metadata cache provided by the client, as
     * SystemMetadata is mutable and so caching can lead to issues.  In specific
     * cases where a client wants to utilize the same system metadata in rapid succession,
     * it may make sense to temporarily use the local cache by calling @see #getSystemMetadata(Session, Identifier, boolean).
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.getSystemMetadata"> DataONE API Reference (MemberNode API)</a> 
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.getSystemMetadata"> DataONE API Reference (CoordinatingNode API)</a> 
     */
    public SystemMetadata getSystemMetadata(Identifier pid)
    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented 
    {
        return getSystemMetadata(this.session, pid, false);
    }

    /**
     * Get the system metadata from a resource with the specified guid, potentially using the local
     * system metadata cache if specified to do so. Used by both the CNode and MNode implementations. 
     * Because SystemMetadata is mutable, caching can lead to currency issues.  In specific
     * cases where a client wants to utilize the same system metadata in rapid succession,
     * it may make sense to temporarily use the local cache by setting useSystemMetadadataCache to true.
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.getSystemMetadata"> DataONE API Reference (MemberNode API)</a> 
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.getSystemMetadata"> DataONE API Reference (CoordinatingNode API)</a> 
     */
	public SystemMetadata getSystemMetadata(Identifier pid, boolean useSystemMetadataCache)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented 
	{
		return getSystemMetadata(this.session, pid, useSystemMetadataCache);
	}
    
    
    
    /**
     * Get the system metadata from a resource with the specified guid. Used
     * by both the CNode and MNode implementations. Note that this method defaults
     * to not using the local system metadata cache provided by the client, as
     * SystemMetadata is mutable and so caching can lead to issues.  In specific
     * cases where a client wants to utilize the same system metadata in rapid succession,
     * it may make sense to temporarily use the local cache by calling @see #getSystemMetadata(Session, Identifier, boolean).
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.getSystemMetadata"> DataONE API Reference (MemberNode API)</a> 
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.getSystemMetadata"> DataONE API Reference (CoordinatingNode API)</a> 
     */
    public SystemMetadata getSystemMetadata(Session session, Identifier pid)
    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented 
    {
        return getSystemMetadata(session, pid, false);
    }

    /**
     * Get the system metadata from a resource with the specified guid, potentially using the local
     * system metadata cache if specified to do so. Used by both the CNode and MNode implementations. 
     * Because SystemMetadata is mutable, caching can lead to currency issues.  In specific
     * cases where a client wants to utilize the same system metadata in rapid succession,
     * it may make sense to temporarily use the local cache by setting useSystemMetadadataCache to true.
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.getSystemMetadata"> DataONE API Reference (MemberNode API)</a> 
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.getSystemMetadata"> DataONE API Reference (CoordinatingNode API)</a> 
     */
	public SystemMetadata getSystemMetadata(Session session, Identifier pid, boolean useSystemMetadataCache)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented 
	{
        boolean cacheMissed = false;

	    if (useSystemMetadataCache) {
            try {
                SystemMetadata sysmeta = LocalCache.instance().getSystemMetadata(pid);
                return sysmeta;
            } catch (NotCached e) {
                cacheMissed = true;
            }
        }
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(),Constants.RESOURCE_META);
		if (pid != null)
			url.addNextPathElement(pid.getValue());

		D1RestClient client = new D1RestClient(session);
		client.setTimeouts(Settings.getConfiguration()
                    .getInteger("D1Client.D1Node.getSystemMetadata.timeout", getDefaultSoTimeout()));
		InputStream is = null;
		SystemMetadata sysmeta = null;
		
		try {
			is = client.doGetRequest(url.getUrl());
			sysmeta = deserializeServiceType(SystemMetadata.class,is);
			if (cacheMissed) {
                // Cache the result in the system metadata cache
                LocalCache.instance().putSystemMetadata(pid, sysmeta);
            }
		} catch (BaseException be) {
            if (be instanceof InvalidToken)      throw (InvalidToken) be;
            if (be instanceof NotAuthorized)     throw (NotAuthorized) be;
            if (be instanceof NotImplemented)    throw (NotImplemented) be;
            if (be instanceof ServiceFailure)    throw (ServiceFailure) be;
            if (be instanceof NotFound)          throw (NotFound) be;
                    
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
        return sysmeta;
	}

	
	public DescribeResponse describe(Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
		return describe(this.session, pid);
    }
	
	
    public DescribeResponse describe(Session session, Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
    	
    	// need to check for null or empty values, or else the call transposes
    	// into a listObjects
    	if(pid == null || pid.getValue().trim().equals(""))
    		throw new NotFound("0000", "'pid' cannot be null nor empty");
    	url.addNextPathElement(pid.getValue());
    	
     	D1RestClient client = new D1RestClient(session);
    	
    	Header[] headers = null;
    	Map<String, String> headersMap = new HashMap<String,String>();
    	try {
    		headers = client.doHeadRequest(url.getUrl());
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
    throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    { 
    	return getChecksum(this.session, pid, checksumAlgorithm);
    }
    
    
    /**
     * This method can handle both the MN and CN method, although the CN overriding method
     * will need to recast the InvalidRequest exception and use 'null' for the checksumAlgorithm param
     * @param session
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
    throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {    	
    	if (pid == null)
            throw new NotFound("0000", "PID cannot be null");
    	
    	// assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_CHECKSUM);
        url.addNextPathElement(pid.getValue());
        
    	url.addNonEmptyParamPair("checksumAlgorithm", checksumAlgorithm);

        // send the request
        D1RestClient client = new D1RestClient(session);
        Checksum checksum = null;

        try {
        	InputStream is = client.doGetRequest(url.getUrl());
        	checksum = deserializeServiceType(Checksum.class, is);
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

        finally {
        	setLatestRequestUrl(client.getLatestRequestUrl());
        	client.closeIdleConnections();
        }
        return checksum;
    }
    
	
	
    public boolean isAuthorized(Identifier pid, Permission action)
    throws ServiceFailure, InvalidRequest, InvalidToken, NotFound, NotAuthorized, NotImplemented
    {
    	return isAuthorized(this.session, pid, action);
    }
    
	
    public boolean isAuthorized(Session session, Identifier pid, Permission action)
    throws ServiceFailure, InvalidRequest, InvalidToken, NotFound, NotAuthorized, NotImplemented
    {
        // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_AUTHORIZATION);
        if (pid != null)
        	url.addNextPathElement(pid.getValue());
        if (action != null)
        	url.addNonEmptyParamPair("action", action.xmlValue());
    	
        // send the request
        D1RestClient client = new D1RestClient(session);
        
        InputStream is = null;
        try {
        	is = client.doGetRequest(url.getUrl());
        	if (is != null)
				is.close();
        } catch (BaseException be) {
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof NotFound)               throw (NotFound) be;
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
        return true;
    }

    
	public  Identifier generateIdentifier(String scheme, String fragment)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest
	{
		return generateIdentifier(this.session, scheme, fragment);
	}
	
    
	public  Identifier generateIdentifier(Session session, String scheme, String fragment)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest
	{
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
		// send the request
		D1RestClient client = new D1RestClient(session);
		Identifier identifier = null;

		try {
			InputStream is = client.doPostRequest(url.getUrl(),smpe);
			identifier = deserializeServiceType(Identifier.class, is);
			
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

		finally {
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
 		return identifier;
	}
    
  
    public  Identifier archive(Identifier pid)
    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
    {
    	return archive(this.session, pid);
    }
	
	
    /**
     *  sets the archived flag to true on an MN or CN
     * @param session
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
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
    {
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ARCHIVE);
    	if (pid != null)
    		url.addNextPathElement(pid.getValue());

     	D1RestClient client = new D1RestClient(session);

     	Identifier identifier = null;
    	try {
    		InputStream is = client.doPutRequest(url.getUrl(), null);
    		identifier = deserializeServiceType(Identifier.class, is);
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
        return identifier;
    }
    
 
    public  Identifier delete(Identifier pid)
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
    {
    	return delete(this.session, pid);
    }
    
    
    public  Identifier delete(Session session, Identifier pid)
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
    {
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
    	if (pid != null)
    		url.addNextPathElement(pid.getValue());

     	D1RestClient client = new D1RestClient(session);

     	Identifier identifier = null;
    	try {
    		InputStream is = client.doDeleteRequest(url.getUrl());
    		identifier = deserializeServiceType(Identifier.class, is);
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
        return identifier;
    }
    
    
	public InputStream query(String queryEngine, String query)
			throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
			NotImplemented, NotFound 
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_QUERY);
        try {
        	url.addNextPathElement(queryEngine);
//        	url.addNextPathElement(query);
        } catch (IllegalArgumentException e) {
       		throw new NotFound("0000", "Neither 'queryEngine' nor 'query' can be null or empty");
       	}
        String finalUrl = url.getUrl() + "/" + query;
        D1RestClient client = new D1RestClient(session);
        InputStream is = null;
        try {
        	is = client.doGetRequest(finalUrl);
		}
        catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;
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
		return is;
	}



	public QueryEngineDescription getQueryEngineDescription(String queryEngine)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented,
			NotFound 
	{
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_QUERY);
        try {
        	url.addNextPathElement(queryEngine);
        } catch (IllegalArgumentException e) {
       		throw new NotFound("0000", "'queryEngine' cannot be null nor empty");
       	}

        D1RestClient client = new D1RestClient(session);
        QueryEngineDescription description = null;
        try {
        	InputStream is = client.doGetRequest(url.getUrl());
             description = deserializeServiceType(QueryEngineDescription.class, is);
		}
        catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
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
		return description;
	}



	public QueryEngineList listQueryEngines() throws InvalidToken, ServiceFailure,
			NotAuthorized, NotImplemented {
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_QUERY);
        
		InputStream is = null;
        D1RestClient client = new D1RestClient(session);
        QueryEngineList engines = null;
        try {
             is = client.doGetRequest(url.getUrl());
             engines = deserializeServiceType(QueryEngineList.class, is);
		}
        catch (BaseException be) {
			if (be instanceof NotImplemented)         throw (NotImplemented) be;
			if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
			if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
			if (be instanceof InvalidToken)           throw (InvalidToken) be;

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
		return engines;
	}
    
    


    /**
     * A helper function to preserve the stackTrace when catching one error and throwing a new one.
     * Also has some descriptive text which makes it clientSide specific
     * @param e
     * @return
     */
    protected static ServiceFailure recastClientSideExceptionToServiceFailure(Exception e) {
    	ServiceFailure sfe = new ServiceFailure("0 Client_Error", e.getClass() + ": "+ e.getMessage());
		sfe.setStackTrace(e.getStackTrace());
    	return sfe;
    }

    
    /**
     * A helper function for recasting DataONE exceptions to ServiceFailures while
     * preserving the detail code and TraceDetails.

     * @param be - BaseException subclass to be recast
     * @return ServiceFailure
     */
    protected static ServiceFailure recastDataONEExceptionToServiceFailure(BaseException be) {	
    	ServiceFailure sfe = new ServiceFailure(be.getDetail_code(), 
    			"Recasted unexpected exception from the service - " + be.getClass() + ": "+ be.getMessage());
    	
    	Iterator<String> it = be.getTraceKeySet().iterator();
    	while (it.hasNext()) {
    		String key = it.next();
    		sfe.addTraceDetail(key, be.getTraceDetail(key));
    	}
    	return sfe;
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
	throws ServiceFailure
	{
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

    public Integer getDefaultSoTimeout() {
        return defaultSoTimeout;
    }

    public void setDefaultSoTimeout(Integer defaultSoTimeout) {
        this.defaultSoTimeout = defaultSoTimeout;
    }
        
}
