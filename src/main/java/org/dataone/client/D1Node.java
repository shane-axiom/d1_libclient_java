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
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpException;
import org.apache.http.client.ClientProtocolException;
import org.dataone.client.cache.LocalCache;
import org.dataone.client.exception.NotCached;
import org.dataone.configuration.Settings;
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
import org.dataone.service.exceptions.SynchronizationFailed;
import org.dataone.service.exceptions.UnsupportedMetadataType;
import org.dataone.service.exceptions.UnsupportedQueryType;
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.exceptions.VersionMismatch;
import org.dataone.service.types.v1.Event;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Log;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.Constants;
import org.dataone.service.util.D1Url;
import org.dataone.service.util.TypeMarshaller;
import org.jibx.runtime.JiBXException;

/**
 * An abstract node class that contains base functionality shared between 
 * Coordinating Node and Member Node implementations. 
 */
public abstract class D1Node {

	// TODO: This class should implement the MemberNodeAuthorization interface as well
    /** The URL string for the node REST API */
    private String nodeBaseServiceUrl;
    private String nodeId;
    
    private boolean useLocalCache = false;

	/**
	 * Constructor to create a new instance.
	 */
	public D1Node(String nodeBaseServiceUrl) {
	    setNodeBaseServiceUrl(nodeBaseServiceUrl);
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

    
    /**
     * creates a public session object that can be used as a default
     * session object if null is passed into a service api method. 
     * @return 
     */
    protected static Session createPublicSession() {

    	Session session = new Session();
    	Subject sub = new Subject();
    	sub.setValue("public");
    	session.setSubject(sub);
    	return session;
    }   
  
    /**
     * A convenience method for listObjects using no filtering parameters
     * @return
     * @throws InvalidRequest
     * @throws InvalidToken
     * @throws NotAuthorized
     * @throws NotImplemented
     * @throws ServiceFailure
     */
    public ObjectList listObjects(Session session) 
    throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure
    {
    	return listObjects(session,null,null,null,null,null,null);
    }


    /* @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.listObjects */

    public ObjectList listObjects(Session session, Date startTime, Date endTime, 
      ObjectFormatIdentifier formatid, Boolean replicaStatus, Integer start, Integer count) 
    throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure
    {

        // TODO: create JavaDoc and fix doc reference
    	
    	if (endTime != null && startTime != null && !endTime.after(startTime))
			throw new InvalidRequest("1000", "startTime must be before endTime in listObjects() call. "
					+ startTime + " " + endTime);

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
		
		url.addDateParamPair("startTime", startTime);
		url.addDateParamPair("endTime", endTime);
		if (formatid != null) 
			url.addNonEmptyParamPair("objectFormat", formatid.getValue());
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
        InputStream is = null;

        try {
        	is = client.doGetRequest(url.getUrl());
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

        return deserializeServiceType(ObjectList.class, is);
    }

    
	public  Log getLogRecords(Session session, Date fromDate, Date toDate,
			Event event, Integer start, Integer count) 
	throws InvalidToken, InvalidRequest, ServiceFailure,
	NotAuthorized, NotImplemented, InsufficientResources
	{
		// TODO: create JavaDoc and fix doc reference

		D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_LOG);
        if (fromDate == null) {
        	throw new InvalidRequest("","The 'fromDate' parameter cannot be null");
        }
    	url.addDateParamPair("fromDate", fromDate);
    	url.addDateParamPair("toDate", toDate);
    	if (event != null)
    		url.addNonEmptyParamPair("event", event.xmlValue());
    	
		// send the request
		D1RestClient client = new D1RestClient(session);
		InputStream is = null;

		try {
			is = client.doGetRequest(url.getUrl());
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

		return deserializeServiceType(Log.class, is);
	}
    
    
	/**
     * Get the resource with the specified guid.  Used by both the CNode and 
     * MNode subclasses. A LocalCache is used to cache objects in memory and in 
     * a local disk cache if the "D1Client.useLocalCache" configuration property
     * was set to true when the D1Node was created. Otherwise
     * InputStream is the Java native version of D1's OctetStream
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.listObjects
     */
    public InputStream get(Session session, Identifier pid)
    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented 
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
    	url.addNextPathElement(pid.getValue());

		D1RestClient client = new D1RestClient(session);
		
		try {
			is = client.doGetRequest(url.getUrl());
			if (cacheMissed) {
			    // Cache the result, and reset the stream mark
			    byte[] data = IOUtils.toByteArray(is);
			    LocalCache.instance().putData(pid, data);
			    is = new ByteArrayInputStream(data);
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

        return is;
    }
 
    
    /**
     * Get the system metadata from a resource with the specified guid. Used
     * by both the CNode and MNode implementations. Note that this method defaults
     * to not using the local system metadata cache provided by the client, as
     * SystemMetadata is mutable and so cacheing can lead to issues.  In specific
     * cases where a client wants to utilize the same system metadata in rapid succession,
     * it may make sense to temporarily use the local cache by calling @see #getSystemMetadata(Session, Identifier, boolean).
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.getSystemMetadata
     */
    public SystemMetadata getSystemMetadata(Session session, Identifier pid)
    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented 
    {
        return getSystemMetadata(session, pid, false);
    }

    /**
     * Get the system metadata from a resource with the specified guid, potentially using the local
     * system metadata cache if specified to do so. Used by both the CNode and MNode implementations. 
     * Because SystemMetadata is mutable, cacheing can lead to currency issues.  In specific
     * cases where a client wants to utilize the same system metadata in rapid succession,
     * it may make sense to temporarily use the local cache by setting useSystemMetadadataCache to true.
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.getSystemMetadata
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
    	url.addNextPathElement(pid.getValue());

		D1RestClient client = new D1RestClient(session);
		
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
	
        return sysmeta;
	}
	
	
    public boolean isAuthorized(Session session, Identifier pid, Permission action)
    throws ServiceFailure, InvalidRequest, InvalidToken, NotFound, NotAuthorized, NotImplemented
    {

        // TODO: create JavaDoc and fix doc reference
    	
    	if (pid == null || pid.getValue().trim().equals(""))
            throw new InvalidRequest("1761", "PID cannot be null nor empty");

        // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_AUTHORIZATION);
    	url.addNextPathElement(pid.getValue());
    	url.addNonEmptyParamPair("action", action.xmlValue());
    	
        // send the request
        D1RestClient client = new D1RestClient(session);

        try {
        	client.doGetRequest(url.getUrl());
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

        return true;
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
}
