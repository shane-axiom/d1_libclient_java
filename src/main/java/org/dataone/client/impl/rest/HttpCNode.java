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
import org.apache.http.client.params.ClientPNames;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.client.utils.HttpUtils;
import org.dataone.configuration.Settings;
import org.dataone.service.cn.v1.CNAuthorization;
import org.dataone.service.cn.v1.CNCore;
import org.dataone.service.cn.v1.CNIdentity;
import org.dataone.service.cn.v1.CNRead;
import org.dataone.service.cn.v1.CNRegister;
import org.dataone.service.cn.v1.CNReplication;
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
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.exceptions.VersionMismatch;
import org.dataone.service.types.v1.Event;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Log;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.ObjectLocationList;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationPolicy;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.D1Url;

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
public class HttpCNode extends CNode 
implements CNCore, CNRead, CNAuthorization, CNIdentity, CNRegister, CNReplication 
{
	protected static org.apache.commons.logging.Log log = LogFactory.getLog(HttpCNode.class);
	
	/** default Socket timeout in milliseconds **/
	private Integer defaultSoTimeout = 30000;
	
	private static final Integer TIMEOUT_SECONDS = 30;
	
    private static final String REPLICATION_TIMEOUT_PROPERTY = "D1Client.CNode.replication.timeout";
	
	/**
	 * Construct a Coordinating Node, passing in the base url for node services. The CN
	 * first retrieves a list of other nodes that can be used to look up node
	 * identifiers and base urls for further service invocations.
	 *
	 * @param nodeBaseServiceUrl base url for constructing service endpoints.
	 */
	public HttpCNode(MultipartRestClient mrc, String nodeBaseServiceUrl) {
		super(nodeBaseServiceUrl);
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
	public HttpCNode(String nodeBaseServiceUrl, Session session) {
		super(nodeBaseServiceUrl, session);
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
		return this.getLogRecords(null);
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
		return this.getLogRecords(session,null, null, null, null, null, null);
	}
	


	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.getLogRecords">see DataONE API Reference</a> } 
	 */
	public  Log getLogRecords(Date fromDate, Date toDate,
			Event event, String pidFilter, Integer start, Integer count) 
	throws InvalidToken, InvalidRequest, ServiceFailure,
	NotAuthorized, NotImplemented, InsufficientResources
	{
		return this.getLogRecords(null, fromDate, toDate, event, pidFilter, start, count);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.getLogRecords">see DataONE API Reference</a> } 
	 */
	public  Log getLogRecords(Session session, Date fromDate, Date toDate,
			Event event, String pidFilter, Integer start, Integer count) 
	throws InvalidToken, InvalidRequest, ServiceFailure,
	NotAuthorized, NotImplemented, InsufficientResources
	{
		RequestConfig previous = setTimeouts(Settings.getConfiguration()
				.getInteger("D1Client.D1Node.getLogRecords.timeout", getDefaultSoTimeout()));
		
		Log log = super.getLogRecords(session, fromDate, toDate, event, pidFilter, start, count);
		((HttpMultipartRestClient)this.restClient).setRequestConfig(previous);
		return log;
	}
		

	


	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.reserveIdentifier">see DataONE API Reference</a> } 
	 */
	public Identifier reserveIdentifier(Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, 
	NotImplemented, InvalidRequest
	{
		return this.reserveIdentifier(this.session, pid);
	}
    
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.reserveIdentifier">see DataONE API Reference</a> } 
	 */
	public Identifier reserveIdentifier(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, 
	NotImplemented, InvalidRequest
	{
		RequestConfig previous = setTimeouts(Settings.getConfiguration()
				.getInteger("D1Client.CNode.reserveIdentifier.timeout", getDefaultSoTimeout()));

 		Identifier id = super.reserveIdentifier(session, pid);
 		((HttpMultipartRestClient)this.restClient).setRequestConfig(previous);
 		return id;
	}


	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.create">see DataONE API Reference</a> } 
	 */
	public Identifier create(Identifier pid, InputStream object,
			SystemMetadata sysmeta) 
	throws InvalidToken, ServiceFailure,NotAuthorized, IdentifierNotUnique, UnsupportedType,
	InsufficientResources, InvalidSystemMetadata, NotImplemented, InvalidRequest
	{
		return this.create(this.session, pid, object, sysmeta);
	}
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.create">see DataONE API Reference</a> } 
	 */
	public Identifier create(Session session, Identifier pid, InputStream object,
			SystemMetadata sysmeta) 
	throws InvalidToken, ServiceFailure,NotAuthorized, IdentifierNotUnique, UnsupportedType,
	InsufficientResources, InvalidSystemMetadata, NotImplemented, InvalidRequest
	{
		RequestConfig previous = setTimeouts(Settings.getConfiguration()
			.getInteger("D1Client.CNode.create.timeout", getDefaultSoTimeout()));
 
        Identifier id = super.create(null,  object, sysmeta);
        ((HttpMultipartRestClient)this.restClient).setRequestConfig(previous);
        return id;
	}


	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.registerSystemMetadata">see DataONE API Reference</a> } 
	 */
	public Identifier registerSystemMetadata( Identifier pid, SystemMetadata sysmeta) 
	throws NotImplemented, NotAuthorized,ServiceFailure, InvalidRequest, 
	InvalidSystemMetadata, InvalidToken
	{
		return this.registerSystemMetadata(this.session, pid,sysmeta);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.registerSystemMetadata">see DataONE API Reference</a> } 
	 */
	public Identifier registerSystemMetadata(Session session, Identifier pid,
		SystemMetadata sysmeta) 
	throws NotImplemented, NotAuthorized,ServiceFailure, InvalidRequest, 
	InvalidSystemMetadata, InvalidToken
	{
		RequestConfig previous = setTimeouts(Settings.getConfiguration()
			.getInteger("D1Client.CNode.registerSystemMetadata.timeout", getDefaultSoTimeout()));

 		Identifier id = super.registerSystemMetadata(session, pid, sysmeta);
 		((HttpMultipartRestClient)this.restClient).setRequestConfig(previous);
 		return id;
	}

	

	////////////////   CN READ API  //////////////

	/**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.listObjects">see DataONE API Reference</a> }
     */
	public ObjectList listObjects() 
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure 
	{
		return this.listObjects(null);
	}
	
	
	/**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.listObjects">see DataONE API Reference</a> }
     */
	@Override
	public ObjectList listObjects(Session session) 
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure 
	{
		return this.listObjects(session, null, null, null, null, null, null);
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
		return this.listObjects(null,fromDate,toDate,formatid,replicaStatus,start,count);
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
		RequestConfig previous = setTimeouts(Settings.getConfiguration()
				.getInteger("D1Client.D1Node.listObjects.timeout", getDefaultSoTimeout()));
		
		ObjectList ol = super.listObjects(session,fromDate,toDate,formatid,replicaStatus,start,count);
		((HttpMultipartRestClient)this.restClient).setRequestConfig(previous);
		return ol;
	}
	

	public InputStream get(Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		return this.get(null, pid);

	}
	
	public InputStream get(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		RequestConfig previous = setTimeouts(Settings.getConfiguration()
				.getInteger("D1Client.D1Node.get.timeout", getDefaultSoTimeout()));

		InputStream is =  super.get(session, pid);
		((HttpMultipartRestClient)this.restClient).setRequestConfig(previous);
		return is;
	}



	public SystemMetadata getSystemMetadata(Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		return this.getSystemMetadata(null, pid);
	}
	
	public SystemMetadata getSystemMetadata(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		RequestConfig previous = setTimeouts(Settings.getConfiguration()
                .getInteger("D1Client.D1Node.getSystemMetadata.timeout", getDefaultSoTimeout()));
		SystemMetadata smd =  super.getSystemMetadata(session, pid);
		((HttpMultipartRestClient)this.restClient).setRequestConfig(previous);
		return smd;
	}



	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.resolve">see DataONE API Reference</a> } 
	 */
	public ObjectLocationList resolve(Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
		return this.resolve(this.session, pid);
	}
    
    /**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.resolve">see DataONE API Reference</a> } 
	 */
	public ObjectLocationList resolve(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{

        // TODO: Refactor away from reliance on implementations
		if (this.restClient instanceof HttpMultipartRestClient) {
			((HttpMultipartRestClient)this.restClient).getHttpClient().getParams().setParameter(ClientPNames.HANDLE_REDIRECTS, false);
		}

 		return super.resolve(session, pid);
// 		((HttpMultipartRestClient)this.restClient).setRequestConfig(previous);
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
		return this.search(this.session, queryType, queryD1url);
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
	 *                     that will be passed to the indicated queryType.  
	 */
	public  ObjectList search(Session session, String queryType, D1Url queryD1url)
	throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest, 
	NotImplemented
	{
		String pathAndQueryString = queryD1url.getUrl();
		return this.search(session, queryType, pathAndQueryString);
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
		return this.search(this.session, queryType, query);
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
	throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest, NotImplemented
	{

		RequestConfig previous = setTimeouts(Settings.getConfiguration()
			.getInteger("D1Client.CNode.search.timeout", getDefaultSoTimeout()));
		ObjectList ol =  super.search(session, queryType, query);
		((HttpMultipartRestClient)this.restClient).setRequestConfig(previous);
		return ol;
	}

	
	////////// CN Authorization API //////////////
	
	//////////  CN IDENTITY API  ///////////////
	


	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRegister.register">see DataONE API Reference</a> }
	 */
	public NodeReference register(Node node)
	throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, 
	IdentifierNotUnique, InvalidToken
	{
		return register(this.session, node);
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
		return this.setReplicationStatus(this.session, pid, nodeRef, status, failure);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.setReplicationStatus">see DataONE API Reference</a> }
	 */
	public boolean setReplicationStatus(Session session, Identifier pid, 
			NodeReference nodeRef, ReplicationStatus status, BaseException failure) 
					throws ServiceFailure, NotImplemented, InvalidToken, NotAuthorized, 
					InvalidRequest, NotFound
	{
		RequestConfig previous = setTimeouts(Settings.getConfiguration().getInteger(
                REPLICATION_TIMEOUT_PROPERTY, getDefaultSoTimeout()));

		boolean b = super.setReplicationStatus(session, pid, nodeRef, status, failure);
		((HttpMultipartRestClient)this.restClient).setRequestConfig(previous);
		return b;
	}


	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.setReplicationPolicy">see DataONE API Reference</a> }
	 */
	public boolean setReplicationPolicy(Identifier pid, ReplicationPolicy policy, long serialVersion) 
		throws NotImplemented, NotFound, NotAuthorized, ServiceFailure, 
		InvalidRequest, InvalidToken, VersionMismatch
	{
		return this.setReplicationPolicy(this.session, pid, policy, serialVersion);
		
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.setReplicationPolicy">see DataONE API Reference</a> }
	 */
	public boolean setReplicationPolicy(Session session, Identifier pid, 
		ReplicationPolicy policy, long serialVersion) 
	throws NotImplemented, NotFound, NotAuthorized, ServiceFailure, 
		InvalidRequest, InvalidToken, VersionMismatch
	{
		RequestConfig previous = setTimeouts(Settings.getConfiguration().getInteger(
                REPLICATION_TIMEOUT_PROPERTY, getDefaultSoTimeout()));

		boolean b = super.setReplicationPolicy(session, pid, policy, serialVersion);
		((HttpMultipartRestClient)this.restClient).setRequestConfig(previous);
		return b;
	}


	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.updateReplicationMetadata">see DataONE API Reference</a> }
	 */
	public boolean updateReplicationMetadata( Identifier pid, Replica replicaMetadata, long serialVersion)
	throws NotImplemented, NotAuthorized, ServiceFailure, NotFound, 
		InvalidRequest, InvalidToken, VersionMismatch
	{
		return this.updateReplicationMetadata(this.session, pid, replicaMetadata, serialVersion);
	}

	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.updateReplicationMetadata">see DataONE API Reference</a> }
	 */
	public boolean updateReplicationMetadata(Session session, 
			Identifier pid, Replica replicaMetadata, long serialVersion)
	throws NotImplemented, NotAuthorized, ServiceFailure, NotFound, 
	InvalidRequest, InvalidToken, VersionMismatch
	{

		RequestConfig previous = setTimeouts(Settings.getConfiguration().getInteger( REPLICATION_TIMEOUT_PROPERTY,
                getDefaultSoTimeout()));

		boolean b =  super.updateReplicationMetadata(session, pid, replicaMetadata, serialVersion);
		((HttpMultipartRestClient)this.restClient).setRequestConfig(previous);
		return b;
	}


	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.deleteReplicationMetadata">see DataONE API Reference</a> }
	 * @throws InvalidRequest 
	 */
	public boolean deleteReplicationMetadata(Identifier pid, NodeReference nodeId, long serialVersion) 
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented,
			VersionMismatch, InvalidRequest 
	{
		return this.deleteReplicationMetadata(this.session, pid,nodeId, serialVersion);
	}
	
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.deleteReplicationMetadata">see DataONE API Reference</a> }
	 * @throws InvalidRequest 
	 */
	@Override
	public boolean deleteReplicationMetadata(Session session, Identifier pid,
			NodeReference nodeId, long serialVersion) throws InvalidToken,
			ServiceFailure, NotAuthorized, NotFound, NotImplemented,
			VersionMismatch, InvalidRequest 
	{
		RequestConfig previous = setTimeouts(Settings.getConfiguration().getInteger(
                REPLICATION_TIMEOUT_PROPERTY, getDefaultSoTimeout()));
		
		boolean b = super.deleteReplicationMetadata(session, pid, nodeId, serialVersion);
		((HttpMultipartRestClient)this.restClient).setRequestConfig(previous);
		return b;
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
