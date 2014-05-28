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

import org.apache.commons.logging.LogFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.DescribeResponse;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Session;

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
public abstract class HttpD1Node extends D1Node {

	protected static org.apache.commons.logging.Log log = LogFactory.getLog(HttpD1Node.class);
	
    /** The URL string for the node REST API */
	protected MultipartRestClient restClient;
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
 	public HttpD1Node(MultipartRestClient client, String nodeBaseServiceUrl, Session session) {
 	    setNodeBaseServiceUrl(nodeBaseServiceUrl);
 	    this.restClient = client;
 	    this.session = session;
 	    this.useLocalCache = Settings.getConfiguration().getBoolean("D1Client.useLocalCache",useLocalCache);
 	}
    
	/**
	 * Constructor to create a new instance.
	 */
	public HttpD1Node(MultipartRestClient client, String nodeBaseServiceUrl) {
	    setNodeBaseServiceUrl(nodeBaseServiceUrl);
	    this.restClient = client;
	    this.session = null;
	    this.useLocalCache = Settings.getConfiguration().getBoolean("D1Client.useLocalCache",useLocalCache);
	}
 	
 	
    
    /**
	 * Constructor to create a new instance.
	 */
	public HttpD1Node(String nodeBaseServiceUrl, Session session) {
	    setNodeBaseServiceUrl(nodeBaseServiceUrl);
	    this.restClient = new DefaultD1RestClient();
	    this.session = session;
	    this.useLocalCache = Settings.getConfiguration().getBoolean("D1Client.useLocalCache",useLocalCache);
	}
    
    
    
	/**
	 * Constructor to create a new instance.
	 */
	public HttpD1Node(String nodeBaseServiceUrl) {
	    setNodeBaseServiceUrl(nodeBaseServiceUrl);
	    this.restClient = new DefaultD1RestClient();
	    this.session = null;
	    this.useLocalCache = Settings.getConfiguration().getBoolean("D1Client.useLocalCache",useLocalCache);
	}

	// TODO: this constructor should not exist
	// lest we end up with a client that is not attached to a particular node; 
	// No code calls it in Java, but it is called by the R client; evaluate if this can change
	/**
	 * default constructor needed by some clients.  This constructor will probably
	 * go away so don't depend on it.  Use public D1Node(String nodeBaseServiceUrl) instead.
	 */
	@Deprecated
	public HttpD1Node() {
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

	
//	/**
//     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.listObjects">see DataONE API Reference</a> }
//     */
//	public ObjectList listObjects() 
//			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
//			ServiceFailure 
//	{
//		return listObjects(this.session);
//	}
//	
//	
//    public ObjectList listObjects(Session session) 
//    throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure
//    {
//    	return listObjects(session,null,null,null,null,null,null);
//    }
//
//	public ObjectList listObjects(Date fromDate,
//			Date toDate, ObjectFormatIdentifier formatid,
//			Boolean replicaStatus, Integer start, Integer count)
//			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
//			ServiceFailure 
//	{
//		return listObjects(this.session,fromDate,toDate,formatid,replicaStatus,start,count);
//	}
//
//
//    public ObjectList listObjects(Session session, Date fromDate, Date toDate, 
//      ObjectFormatIdentifier formatid, Boolean replicaStatus, Integer start, Integer count) 
//    throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure
//    {
// 
//        // send the request
//        setTimeouts(Settings.getConfiguration()
//			.getInteger("D1Client.D1Node.listObjects.timeout", getDefaultSoTimeout()));
//  
//        return super.listObjects(session, fromDate, toDate, formatid, replicaStatus, start, count);
//    }

    
//    public Log getLogRecords() 
//	throws InvalidToken, InvalidRequest, ServiceFailure,
//	NotAuthorized, NotImplemented, InsufficientResources
//	{
//    	return getLogRecords(null, null, null, null, null, null);
//	}   
//    
//    
//    public Log getLogRecords(Session session) 
//	throws InvalidToken, InvalidRequest, ServiceFailure,
//	NotAuthorized, NotImplemented, InsufficientResources
//	{
//    	return getLogRecords(session, null, null, null, null, null, null);
//	}
//    
//	
//	public Log getLogRecords(Date fromDate, Date toDate,
//			Event event, String pidFilter, Integer start, Integer count) 
//	throws InvalidToken, InvalidRequest, ServiceFailure,
//	NotAuthorized, NotImplemented, InsufficientResources
//	{
//		return getLogRecords(this.session, fromDate, toDate, event, pidFilter, start, count);
//	}
//  
//	
//	public Log getLogRecords(Session session, Date fromDate, Date toDate,
//			Event event, String pidFilter, Integer start, Integer count) 
//	throws InvalidToken, InvalidRequest, ServiceFailure,
//	NotAuthorized, NotImplemented, InsufficientResources
//	{
//
//		setTimeouts(Settings.getConfiguration()
//			.getInteger("D1Client.D1Node.getLogRecords.timeout", getDefaultSoTimeout()));
//		
//		return super.getLogRecords(session, fromDate, toDate, event, pidFilter, start, count);
//	}
    
	
//	/**
//     * Get the resource with the specified pid.  Used by both the CNode and 
//     * MNode subclasses. A LocalCache is used to cache objects in memory and in 
//     * a local disk cache if the "D1Client.useLocalCache" configuration property
//     * was set to true when the D1Node was created. Otherwise
//     * InputStream is the Java native version of D1's OctetStream
//     * 
//     * @see <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.get">see DataONE API Reference (MemberNode API)</a>
//     * @see <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.get">see DataONE API Reference (CoordinatingNode API)</a>
//     */
//	public InputStream get(Identifier pid)
//    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, 
//      NotImplemented, InsufficientResources
//    {
//		return get(this.session, pid);
//    }
//	
//	
//	/**
//     * Get the resource with the specified pid.  Used by both the CNode and 
//     * MNode subclasses. A LocalCache is used to cache objects in memory and in 
//     * a local disk cache if the "D1Client.useLocalCache" configuration property
//     * was set to true when the D1Node was created. Otherwise
//     * InputStream is the Java native version of D1's OctetStream
//     * 
//     * @see <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.get">see DataONE API Reference (MemberNode API)</a>
//     * @see <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.get">see DataONE API Reference (CoordinatingNode API)</a>
//     */
//    public InputStream get(Session session, Identifier pid)
//    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, 
//      NotImplemented, InsufficientResources
//    {
//        setTimeouts(Settings.getConfiguration()
//                .getInteger("D1Client.D1Node.get.timeout", getDefaultSoTimeout()));
//      
//        return super.get(session, pid);
//    }
 
    
    
//    /**
//     * Get the system metadata from a resource with the specified guid. Used
//     * by both the CNode and MNode implementations. Note that this method defaults
//     * to not using the local system metadata cache provided by the client, as
//     * SystemMetadata is mutable and so caching can lead to issues.  In specific
//     * cases where a client wants to utilize the same system metadata in rapid succession,
//     * it may make sense to temporarily use the local cache by calling @see #getSystemMetadata(Session, Identifier, boolean).
//     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.getSystemMetadata"> DataONE API Reference (MemberNode API)</a> 
//     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.getSystemMetadata"> DataONE API Reference (CoordinatingNode API)</a> 
//     */
//    public SystemMetadata getSystemMetadata(Identifier pid)
//    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented 
//    {
//        return getSystemMetadata(this.session, pid, false);
//    }
//
//    /**
//     * Get the system metadata from a resource with the specified guid, potentially using the local
//     * system metadata cache if specified to do so. Used by both the CNode and MNode implementations. 
//     * Because SystemMetadata is mutable, caching can lead to currency issues.  In specific
//     * cases where a client wants to utilize the same system metadata in rapid succession,
//     * it may make sense to temporarily use the local cache by setting useSystemMetadadataCache to true.
//     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.getSystemMetadata"> DataONE API Reference (MemberNode API)</a> 
//     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.getSystemMetadata"> DataONE API Reference (CoordinatingNode API)</a> 
//     */
//	public SystemMetadata getSystemMetadata(Identifier pid, boolean useSystemMetadataCache)
//	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented 
//	{
//		return getSystemMetadata(this.session, pid, useSystemMetadataCache);
//	}
//    
//    
//    
//    /**
//     * Get the system metadata from a resource with the specified guid. Used
//     * by both the CNode and MNode implementations. Note that this method defaults
//     * to not using the local system metadata cache provided by the client, as
//     * SystemMetadata is mutable and so caching can lead to issues.  In specific
//     * cases where a client wants to utilize the same system metadata in rapid succession,
//     * it may make sense to temporarily use the local cache by calling @see #getSystemMetadata(Session, Identifier, boolean).
//     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.getSystemMetadata"> DataONE API Reference (MemberNode API)</a> 
//     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.getSystemMetadata"> DataONE API Reference (CoordinatingNode API)</a> 
//     */
//    public SystemMetadata getSystemMetadata(Session session, Identifier pid)
//    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented 
//    {
//        return getSystemMetadata(session, pid, false);
//    }
//
//    /**
//     * Get the system metadata from a resource with the specified guid, potentially using the local
//     * system metadata cache if specified to do so. Used by both the CNode and MNode implementations. 
//     * Because SystemMetadata is mutable, caching can lead to currency issues.  In specific
//     * cases where a client wants to utilize the same system metadata in rapid succession,
//     * it may make sense to temporarily use the local cache by setting useSystemMetadadataCache to true.
//     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.getSystemMetadata"> DataONE API Reference (MemberNode API)</a> 
//     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.getSystemMetadata"> DataONE API Reference (CoordinatingNode API)</a> 
//     */
//	public SystemMetadata getSystemMetadata(Session session, Identifier pid, boolean useSystemMetadataCache)
//	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented 
//	{		
//		setTimeouts(Settings.getConfiguration()
//                    .getInteger("D1Client.D1Node.getSystemMetadata.timeout", getDefaultSoTimeout()));
//		
//        return super.getSystemMetadata(session, pid, useSystemMetadataCache);
//	}

	
	public DescribeResponse describe(Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
		return describe(this.session, pid);
    }
	
	


//
//    /**
//     * A helper function to preserve the stackTrace when catching one error and throwing a new one.
//     * Also has some descriptive text which makes it clientSide specific
//     * @param e
//     * @return
//     */
//    protected static ServiceFailure recastClientSideExceptionToServiceFailure(Exception e) {
//    	ServiceFailure sfe = new ServiceFailure("0 Client_Error", e.getClass() + ": "+ e.getMessage());
//                sfe.initCause(e);
//		sfe.setStackTrace(e.getStackTrace());
//    	return sfe;
//    }
//
//    
//    /**
//     * A helper function for recasting DataONE exceptions to ServiceFailures while
//     * preserving the detail code and TraceDetails.
//
//     * @param be - BaseException subclass to be recast
//     * @return ServiceFailure
//     */
//    protected static ServiceFailure recastDataONEExceptionToServiceFailure(BaseException be) {	
//    	ServiceFailure sfe = new ServiceFailure(be.getDetail_code(), 
//    			"Recasted unexpected exception from the service - " + be.getClass() + ": "+ be.getMessage());
//    	
//    	Iterator<String> it = be.getTraceKeySet().iterator();
//    	while (it.hasNext()) {
//    		String key = it.next();
//    		sfe.addTraceDetail(key, be.getTraceDetail(key));
//    	}
//    	return sfe;
//    }
//
//    
//	/**
//	 * deserialize an object of type from the inputstream
//	 * This is a wrapper method of the standard common Unmarshalling method
//	 * that recasts exceptions to ServiceFailure
//	 * 
//	 * @param type
//	 *            the class of the object to serialize (i.e.
//	 *            SystemMetadata.class)
//	 * @param is
//	 *            the stream to deserialize from
//	 * @throws ServiceFailure 
//	 */
//	@SuppressWarnings("rawtypes")
//	protected <T> T deserializeServiceType(Class<T> domainClass, InputStream is)
//	throws ServiceFailure
//	{
//		try {
//			return TypeMarshaller.unmarshalTypeFromStream(domainClass, is);
//		} catch (JiBXException e) {
//            throw new ServiceFailure("0",
//                    "Could not deserialize the " + domainClass.getCanonicalName() + ": " + e.getMessage());
//        } catch (IOException e) {
//        	throw new ServiceFailure("0",
//                    "Could not deserialize the " + domainClass.getCanonicalName() + ": " + e.getMessage());
//		} catch (InstantiationException e) {
//			throw new ServiceFailure("0",
//                    "Could not deserialize the " + domainClass.getCanonicalName() + ": " + e.getMessage());
//		} catch (IllegalAccessException e) {
//			throw new ServiceFailure("0",
//                    "Could not deserialize the " + domainClass.getCanonicalName() + ": " + e.getMessage());
//		}
//	}

    public Integer getDefaultSoTimeout() {
        return defaultSoTimeout;
    }

    public void setDefaultSoTimeout(Integer defaultSoTimeout) {
        this.defaultSoTimeout = defaultSoTimeout;
    }
        
}
