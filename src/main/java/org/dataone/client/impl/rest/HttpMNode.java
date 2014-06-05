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

import java.io.InputStream;
import java.util.Date;

import org.apache.commons.logging.LogFactory;
import org.apache.http.client.config.RequestConfig;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.client.utils.HttpUtils;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidSystemMetadata;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.mn.tier1.v1.MNCore;
import org.dataone.service.mn.tier1.v1.MNRead;
import org.dataone.service.mn.tier2.v1.MNAuthorization;
import org.dataone.service.mn.tier3.v1.MNStorage;
import org.dataone.service.mn.tier4.v1.MNReplication;
import org.dataone.service.mn.v1.MNQuery;
import org.dataone.service.types.v1.Event;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Log;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.D1Url;

/**
 * MultipartMNode represents a MemberNode, and exposes the services associated with a
 * DataONE Member Node, allowing calling clients to call the services associated
 * with the node.
 * 
 * By necessity, behavior for null values differs slightly from the REST api. 
 * Except where noted, null values in method parameters will:
 * 1. throw a NotFound or InvalidRequest exception if the parameter is mapped to the URL path segment
 * 2. omit the url query parameter if the parameter is mapped to a URL query segment
 * 3. throw an InvalidRequest exception if the parameter is mapped to the message body
 * 
 * Session objects get passed to the underlying HttpMultipartRestClient class, but  be passed to the httpClient request, as they are
 * pulled from the underlying filesystem of the local machine.
 * 
 * Java implementation of the following types:
 * Types.OctectStream - java.io.InputStream
 * unsignedlong - we use long to keep to a native type, although only half capacity of unsigned
 * Types.DateTime - java.util.Date
 * 
 * Various methods may set their own timeouts by use of Settings.Configuration properties
 * or by calling setDefaultSoTimeout.  Settings.Configuration properties override
 * any value of the DefaultSoTimeout.  Timeouts are always represented in milliseconds
 * 
 * timeout properties recognized:
 * D1Client.MNode.create.timeout
 * D1Client.MNode.update.timeout
 * D1Client.MNode.replicate.timeout
 * D1Client.MNode.getReplica.timeout
 * D1Client.D1Node.listObjects.timeout
 * D1Client.D1Node.getLogRecords.timeout
 * D1Client.D1Node.get.timeout
 * D1Client.D1Node.getSystemMetadata.timeout
 * 
 *  @author Rob Nahf
 */

public class HttpMNode extends MultipartMNode 
implements MNCore, MNRead, MNAuthorization, MNStorage, MNReplication, MNQuery 
{
  
	protected static org.apache.commons.logging.Log log = LogFactory.getLog(HttpMNode.class);
	
	/** default Socket timeout in milliseconds **/
	private Integer defaultSoTimeout = 30000;
	
	
    /**
     * Construct a new client-side MultipartMNode (Member Node) object, 
     * passing in the base url of the member node for calling its services.
     * @param nodeBaseServiceUrl base url for constructing service endpoints.
     */
	@Deprecated
    public HttpMNode(String nodeBaseServiceUrl) {
        super(nodeBaseServiceUrl); 
    }
   
    
    /**
     * Construct a new client-side MultipartMNode (Member Node) object, 
     * passing in the base url of the member node for calling its services,
     * and the Session to use for connections to that node. 
     * @param nodeBaseServiceUrl base url for constructing service endpoints.
     * @param session - the Session object passed to the CertificateManager
     *                  to be used for establishing connections
     */
    @Deprecated
    public HttpMNode(String nodeBaseServiceUrl, Session session) {
        super(nodeBaseServiceUrl, session); 
    }
    
    /**
     * Construct a new client-side MultipartMNode (Member Node) object, 
     * passing in the base url of the member node for calling its services.
     * @param nodeBaseServiceUrl base url for constructing service endpoints.
     */
    public HttpMNode(MultipartRestClient mrc, String nodeBaseServiceUrl) {
        super(mrc, nodeBaseServiceUrl); 
    }
   
    
    /**
     * Construct a new client-side MultipartMNode (Member Node) object, 
     * passing in the base url of the member node for calling its services,
     * and the Session to use for connections to that node. 
     * @param nodeBaseServiceUrl base url for constructing service endpoints.
     * @param session - the Session object passed to the CertificateManager
     *                  to be used for establishing connections
     */
    public HttpMNode(MultipartRestClient mrc, String nodeBaseServiceUrl, Session session) {
        super(mrc, nodeBaseServiceUrl, session); 
    }
    
    
    public String getNodeBaseServiceUrl() {
    	D1Url url = new D1Url(super.getNodeBaseServiceUrl());
    	url.addNextPathElement(MNCore.SERVICE_VERSION);
    	log.debug("Node base service URL is: " + url.getUrl());
    	return url.getUrl();
    }
    

    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNCore.getLogRecords">see DataONE API Reference</a> }
     */
	public Log getLogRecords() 
	throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure 
	{	
    	return this.getLogRecords(this.session);
	}
	
	
    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNCore.getLogRecords">see DataONE API Reference</a> }
     */
	public Log getLogRecords(Session session) 
	throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure 
	{

    	return this.getLogRecords(session, null, null, null, null, null, null);
	}
 

	
    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNCore.getLogRecords">see DataONE API Reference</a> }
     * 
     */
    public Log getLogRecords(Date fromDate, Date toDate, Event event, String pidFilter, 
               Integer start, Integer count) 
    throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure
    {	
    	return this.getLogRecords(this.session, fromDate, toDate, event, pidFilter, start, count);
    }
    
    
    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNCore.getLogRecords">see DataONE API Reference</a> }
     * 
     */
    public Log getLogRecords(Session session, Date fromDate, Date toDate, 
               Event event, String pidFilter, Integer start, Integer count) 
    throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure
    {

    	RequestConfig previous = setTimeouts(Settings.getConfiguration()
				.getInteger("D1Client.D1Node.getLogRecords.timeout", getDefaultSoTimeout()));
    	
    	Log log = super.getLogRecords(session, fromDate, toDate, event, pidFilter, start, count);
    	((HttpMultipartRestClient)this.restClient).setRequestConfig(previous);
    	return log;
    }

    
    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.listObjects">see DataONE API Reference</a> }
     */
	public ObjectList listObjects() 
	throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure 
	{
		return this.listObjects(this.session);
	}
    
    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.listObjects">see DataONE API Reference</a> }
     */
	@Override
	public ObjectList listObjects(Session session) 
	throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure 
	{
		return this.listObjects(session, null, null, null, null, null, null);
	}

	
    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.listObjects">see DataONE API Reference</a> }
     */	
	public ObjectList listObjects(Date fromDate, Date toDate, ObjectFormatIdentifier formatid,
			Boolean replicaStatus, Integer start, Integer count)
	throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure 
	{
		return this.listObjects(this.session, fromDate,toDate,formatid,replicaStatus,start,count);
	}
	
 
	/**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.listObjects">see DataONE API Reference</a> }
     */	
	@Override
	public ObjectList listObjects(Session session, Date fromDate,
			Date toDate, ObjectFormatIdentifier formatid,
			Boolean replicaStatus, Integer start, Integer count)
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure 
	{
		RequestConfig previous = setTimeouts(Settings.getConfiguration()
			.getInteger("D1Client.D1Node.listObjects.timeout", getDefaultSoTimeout()));
        
		ObjectList ol = super.listObjects(session,fromDate,toDate,formatid,replicaStatus,start,count);
		((HttpMultipartRestClient)this.restClient).setRequestConfig(previous);
		return ol;
	}
   

    public InputStream get(Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound, InsufficientResources
    {
    	return this.get(this.session, pid);
    }  

    
    public InputStream get(Session session, Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound, InsufficientResources
    {
    	RequestConfig previous = setTimeouts(Settings.getConfiguration()
                .getInteger("D1Client.D1Node.get.timeout", getDefaultSoTimeout()));
    	InputStream is = super.get(session, pid);
    	return is;
    }


    public SystemMetadata getSystemMetadata(Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
    	return this.getSystemMetadata(this.session, pid);
    }   
    
    public SystemMetadata getSystemMetadata(Session session, Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
    	RequestConfig previous = setTimeouts(Settings.getConfiguration()
                .getInteger("D1Client.D1Node.getSystemMetadata.timeout", getDefaultSoTimeout()));
    	SystemMetadata smd = super.getSystemMetadata(session,pid);
    	((HttpMultipartRestClient)this.restClient).setRequestConfig(previous);
    	return smd;
    }

	
    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.create">see DataONE API Reference</a> } 
     */
    public  Identifier create(Identifier pid, InputStream object, 
            SystemMetadata sysmeta) 
    throws IdentifierNotUnique, InsufficientResources, InvalidRequest, InvalidSystemMetadata, 
        	InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, UnsupportedType
    {
        return this.create(this.session, pid, object,  sysmeta);
    }
     
		
	/**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.create">see DataONE API Reference</a> } 
     */
    public  Identifier create(Session session, Identifier pid, InputStream object, 
            SystemMetadata sysmeta) 
    throws IdentifierNotUnique, InsufficientResources, InvalidRequest, InvalidSystemMetadata, 
        	InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, UnsupportedType
    {

    	RequestConfig previous = setTimeouts(Settings.getConfiguration()
			.getInteger("D1Client.MNode.create.timeout", getDefaultSoTimeout()));

        Identifier id = super.create(session, pid, object, sysmeta);
        ((HttpMultipartRestClient)this.restClient).setRequestConfig(previous);
        return id;
    }


    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.update">see DataONE API Reference</a> } 
     */
    public  Identifier update(Identifier pid, InputStream object, 
            Identifier newPid, SystemMetadata sysmeta) 
        throws IdentifierNotUnique, InsufficientResources, InvalidRequest, InvalidSystemMetadata, 
            InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, UnsupportedType,
            NotFound
    {
        return this.update(this.session, pid, object, newPid, sysmeta);
    }
    
    
    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.update">see DataONE API Reference</a> } 
     */
    public  Identifier update(Session session, Identifier pid, InputStream object, 
            Identifier newPid, SystemMetadata sysmeta) 
        throws IdentifierNotUnique, InsufficientResources, InvalidRequest, InvalidSystemMetadata, 
            InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, UnsupportedType,
            NotFound
    {
    	  	
    	RequestConfig previous = setTimeouts(Settings.getConfiguration()
			.getInteger("D1Client.MNode.update.timeout", getDefaultSoTimeout()));
    	
    	Identifier id = super.update(pid, object, newPid, sysmeta);
    	
    	((HttpMultipartRestClient)this.restClient).setRequestConfig(previous);

        return id;
    }


 
 
    
    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNReplication.replicate">see DataONE API Reference</a> } 
     */
    public boolean replicate(SystemMetadata sysmeta, NodeReference sourceNode) 
    throws NotImplemented, ServiceFailure, NotAuthorized, InvalidRequest, InvalidToken,
        InsufficientResources, UnsupportedType
    {
        return this.replicate(this.session, sysmeta, sourceNode);
    }
    
    
    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNReplication.replicate">see DataONE API Reference</a> } 
     */
    public boolean replicate(Session session, SystemMetadata sysmeta, NodeReference sourceNode) 
    throws NotImplemented, ServiceFailure, NotAuthorized, InvalidRequest, InvalidToken,
        InsufficientResources, UnsupportedType
    {  	
    	RequestConfig previous = setTimeouts(Settings.getConfiguration()
			.getInteger("D1Client.MNode.replicate.timeout", getDefaultSoTimeout()));
      
        boolean b = super.replicate(session, sysmeta, sourceNode);
        ((HttpMultipartRestClient)this.restClient).setRequestConfig(previous);
        return b;
    }


    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.getReplica">see DataONE API Reference</a> } 
     */
    public InputStream getReplica(Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound,
    InsufficientResources
    {
       return this.getReplica(this.session, pid);
    }
    
    
    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.getReplica">see DataONE API Reference</a> } 
     */
    public InputStream getReplica(Session session, Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound,
    InsufficientResources
    {
    	RequestConfig previous = setTimeouts(Settings.getConfiguration()
			.getInteger("D1Client.MNode.getReplica.timeout", getDefaultSoTimeout()));
 
        InputStream is = super.getReplica(session, pid);
        ((HttpMultipartRestClient)this.restClient).setRequestConfig(previous);
        return is;
    }
    
 
    
	
	protected RequestConfig setTimeouts(Integer milliseconds) {
		return HttpUtils.setTimeouts(this.restClient, milliseconds);
	}
	
	public Integer getDefaultSoTimeout() {
		return defaultSoTimeout;
	}

	public void setDefaultSoTimeout(Integer defaultSoTimeout) {
		this.defaultSoTimeout = defaultSoTimeout;
	}

}
