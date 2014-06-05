package org.dataone.client;

import java.io.InputStream;
import java.util.Date;
import java.util.Set;

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
import org.dataone.service.types.v1_1.QueryEngineDescription;
import org.dataone.service.types.v1_1.QueryEngineList;
import org.dataone.service.util.D1Url;

public interface CNode {

	public String getNodeBaseServiceUrl();

	/**
     * @return the nodeId
     */
    public String getNodeId();

    /**
     * @param nodeId the nodeId to set
     */
    public void setNodeId(String nodeId);
    
    
	
//	/**
//	 * Return the set of Node IDs for all of the nodes registered to the CN 
//	 * @return
//	 * @throws NotImplemented 
//	 * @throws ServiceFailure 
//	 */
//	public Set<String> listNodeIds() throws ServiceFailure, NotImplemented;

	/**
	 *  {@link <a href="http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.ping">see DataONE API Reference</a> }
	 */
	public Date ping() throws NotImplemented, ServiceFailure,
			InsufficientResources;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.listFormats">see DataONE API Reference</a> } 
	 */
	public ObjectFormatList listFormats() throws ServiceFailure, NotImplemented;

	/**
	 *  Return the ObjectFormat for the given ObjectFormatIdentifier, obtained 
	 *  either from a client-cached ObjectFormatList from the ObjectFormatCache,
	 *  or from a call to the CN.
	 *  Caching is enabled by default in production via the property 
	 *  "CNode.useObjectFormatCache" accessed via org.dataone.configuration.Settings
	 *  and the cn baseURL found in D1Client.CN_URL is used for the connection, 
	 *  not the one held by this instance of CNode.
	 *  
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.getFormat">see DataONE API Reference</a> } 
	 */
	public ObjectFormat getFormat(ObjectFormatIdentifier formatid)
			throws ServiceFailure, NotFound, NotImplemented;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.getChecksumAlgorithms">see DataONE API Reference</a> } 
	 */
	public ChecksumAlgorithmList listChecksumAlgorithms()
			throws ServiceFailure, NotImplemented;

	/**
	 *  A convenience method for getLogRecords using no filtering parameters
	 *  
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.getLogRecords">see DataONE API Reference</a> } 
	 */
	public Log getLogRecords() throws InvalidToken, InvalidRequest,
			ServiceFailure, NotAuthorized, NotImplemented,
			InsufficientResources;

	/**
	 *  A convenience method for getLogRecords using no filtering parameters
	 *  
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.getLogRecords">see DataONE API Reference</a> } 
	 */
	public Log getLogRecords(Session session) throws InvalidToken,
			InvalidRequest, ServiceFailure, NotAuthorized, NotImplemented,
			InsufficientResources;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.getLogRecords">see DataONE API Reference</a> } 
	 */
	public Log getLogRecords(Date fromDate, Date toDate, Event event,
			String pidFilter, Integer start, Integer count)
			throws InvalidToken, InvalidRequest, ServiceFailure, NotAuthorized,
			NotImplemented, InsufficientResources;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.getLogRecords">see DataONE API Reference</a> } 
	 */
	public Log getLogRecords(Session session, Date fromDate, Date toDate,
			Event event, String pidFilter, Integer start, Integer count)
			throws InvalidToken, InvalidRequest, ServiceFailure, NotAuthorized,
			NotImplemented, InsufficientResources;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.listNodes">see DataONE API Reference</a> } 
	 */
	public NodeList listNodes() throws NotImplemented, ServiceFailure;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.reserveIdentifier">see DataONE API Reference</a> } 
	 */
	public Identifier reserveIdentifier(Identifier pid) throws InvalidToken,
			ServiceFailure, NotAuthorized, IdentifierNotUnique, NotImplemented,
			InvalidRequest;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.reserveIdentifier">see DataONE API Reference</a> } 
	 */
	public Identifier reserveIdentifier(Session session, Identifier pid)
			throws InvalidToken, ServiceFailure, NotAuthorized,
			IdentifierNotUnique, NotImplemented, InvalidRequest;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.generateIdentifier">see DataONE API Reference</a> } 
	 */
	public Identifier generateIdentifier(String scheme, String fragment)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented,
			InvalidRequest;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.generateIdentifier">see DataONE API Reference</a> } 
	 */
	public Identifier generateIdentifier(Session session, String scheme,
			String fragment) throws InvalidToken, ServiceFailure,
			NotAuthorized, NotImplemented, InvalidRequest;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.hasReservation">see DataONE API Reference</a> } 
	 */
	public boolean hasReservation(Subject subject, Identifier pid)
			throws InvalidToken, ServiceFailure, NotFound, NotAuthorized,
			NotImplemented, IdentifierNotUnique;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.hasReservation">see DataONE API Reference</a> } 
	 */
	public boolean hasReservation(Session session, Subject subject,
			Identifier pid) throws InvalidToken, ServiceFailure, NotFound,
			NotAuthorized, NotImplemented, IdentifierNotUnique;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.create">see DataONE API Reference</a> } 
	 */
	public Identifier create(Identifier pid, InputStream object,
			SystemMetadata sysmeta) throws InvalidToken, ServiceFailure,
			NotAuthorized, IdentifierNotUnique, UnsupportedType,
			InsufficientResources, InvalidSystemMetadata, NotImplemented,
			InvalidRequest;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.create">see DataONE API Reference</a> } 
	 */
	public Identifier create(Session session, Identifier pid,
			InputStream object, SystemMetadata sysmeta) throws InvalidToken,
			ServiceFailure, NotAuthorized, IdentifierNotUnique,
			UnsupportedType, InsufficientResources, InvalidSystemMetadata,
			NotImplemented, InvalidRequest;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.registerSystemMetadata">see DataONE API Reference</a> } 
	 */
	public Identifier registerSystemMetadata(Identifier pid,
			SystemMetadata sysmeta) throws NotImplemented, NotAuthorized,
			ServiceFailure, InvalidRequest, InvalidSystemMetadata, InvalidToken;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.registerSystemMetadata">see DataONE API Reference</a> } 
	 */
	public Identifier registerSystemMetadata(Session session, Identifier pid,
			SystemMetadata sysmeta) throws NotImplemented, NotAuthorized,
			ServiceFailure, InvalidRequest, InvalidSystemMetadata, InvalidToken;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.setObsoletedBy">see DataONE API Reference</a> } 
	 */
	public boolean setObsoletedBy(Identifier pid, Identifier obsoletedByPid,
			long serialVersion) throws NotImplemented, NotFound, NotAuthorized,
			ServiceFailure, InvalidRequest, InvalidToken, VersionMismatch;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNCore.setObsoletedBy">see DataONE API Reference</a> } 
	 */
	public boolean setObsoletedBy(Session session, Identifier pid,
			Identifier obsoletedByPid, long serialVersion)
			throws NotImplemented, NotFound, NotAuthorized, ServiceFailure,
			InvalidRequest, InvalidToken, VersionMismatch;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.listObjects">see DataONE API Reference</a> }
	 */
	public ObjectList listObjects() throws InvalidRequest, InvalidToken,
			NotAuthorized, NotImplemented, ServiceFailure;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.listObjects">see DataONE API Reference</a> }
	 */
	public ObjectList listObjects(Session session) throws InvalidRequest,
			InvalidToken, NotAuthorized, NotImplemented, ServiceFailure;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.listObjects">see DataONE API Reference</a> }
	 */
	public ObjectList listObjects(Date fromDate, Date toDate,
			ObjectFormatIdentifier formatid, Boolean replicaStatus,
			Integer start, Integer count) throws InvalidRequest, InvalidToken,
			NotAuthorized, NotImplemented, ServiceFailure;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.listObjects">see DataONE API Reference</a> }
	 */
	public ObjectList listObjects(Session session, Date fromDate, Date toDate,
			ObjectFormatIdentifier formatid, Boolean replicaStatus,
			Integer start, Integer count) throws InvalidRequest, InvalidToken,
			NotAuthorized, NotImplemented, ServiceFailure;

	public InputStream get(Identifier pid) throws InvalidToken, ServiceFailure,
			NotAuthorized, NotFound, NotImplemented;

	public InputStream get(Session session, Identifier pid)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
			NotImplemented;

	public SystemMetadata getSystemMetadata(Identifier pid)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
			NotImplemented;

	public SystemMetadata getSystemMetadata(Session session, Identifier pid)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
			NotImplemented;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.describe">see DataONE API Reference</a> } 
	 */
	public DescribeResponse describe(Identifier pid) throws InvalidToken,
			NotAuthorized, NotImplemented, ServiceFailure, NotFound;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.describe">see DataONE API Reference</a> } 
	 */
	public DescribeResponse describe(Session session, Identifier pid)
			throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure,
			NotFound;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.resolve">see DataONE API Reference</a> } 
	 */
	public ObjectLocationList resolve(Identifier pid) throws InvalidToken,
			ServiceFailure, NotAuthorized, NotFound, NotImplemented;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.resolve">see DataONE API Reference</a> } 
	 */
	public ObjectLocationList resolve(Session session, Identifier pid)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
			NotImplemented;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.getChecksum">see DataONE API Reference</a> } 
	 */
	public Checksum getChecksum(Identifier pid) throws InvalidToken,
			ServiceFailure, NotAuthorized, NotFound, NotImplemented;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.getChecksum">see DataONE API Reference</a> } 
	 */
	public Checksum getChecksum(Session session, Identifier pid)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
			NotImplemented;

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
	public ObjectList search(String queryType, D1Url queryD1url)
			throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
			NotImplemented;

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
	public ObjectList search(Session session, String queryType, D1Url queryD1url)
			throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
			NotImplemented;

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
	public ObjectList search(String queryType, String query)
			throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
			NotImplemented;

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
	public ObjectList search(Session session, String queryType, String query)
			throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
			NotImplemented;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNAuthorization.setRightsHolder">see DataONE API Reference</a> }
	 */
	public Identifier setRightsHolder(Identifier pid, Subject userId,
			long serialVersion) throws InvalidToken, ServiceFailure, NotFound,
			NotAuthorized, NotImplemented, InvalidRequest, VersionMismatch;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNAuthorization.setRightsHolder">see DataONE API Reference</a> }
	 */
	public Identifier setRightsHolder(Session session, Identifier pid,
			Subject userId, long serialVersion) throws InvalidToken,
			ServiceFailure, NotFound, NotAuthorized, NotImplemented,
			InvalidRequest, VersionMismatch;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNAuthorization.isAuthorized">see DataONE API Reference</a> } 
	 */
	public boolean isAuthorized(Identifier pid, Permission permission)
			throws ServiceFailure, InvalidToken, NotFound, NotAuthorized,
			NotImplemented, InvalidRequest;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNAuthorization.isAuthorized">see DataONE API Reference</a> } 
	 */
	public boolean isAuthorized(Session session, Identifier pid,
			Permission permission) throws ServiceFailure, InvalidToken,
			NotFound, NotAuthorized, NotImplemented, InvalidRequest;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNAuthorization.setAccessPolicy">see DataONE API Reference</a> } 
	 */
	public boolean setAccessPolicy(Identifier pid, AccessPolicy accessPolicy,
			long serialVersion) throws InvalidToken, NotFound, NotImplemented,
			NotAuthorized, ServiceFailure, InvalidRequest, VersionMismatch;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNAuthorization.setAccessPolicy">see DataONE API Reference</a> } 
	 */
	public boolean setAccessPolicy(Session session, Identifier pid,
			AccessPolicy accessPolicy, long serialVersion) throws InvalidToken,
			NotFound, NotImplemented, NotAuthorized, ServiceFailure,
			InvalidRequest, VersionMismatch;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.registerAccount">see DataONE API Reference</a> } 
	 */
	public Subject registerAccount(Person person) throws ServiceFailure,
			NotAuthorized, IdentifierNotUnique, InvalidCredentials,
			NotImplemented, InvalidRequest, InvalidToken;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.registerAccount">see DataONE API Reference</a> } 
	 */
	public Subject registerAccount(Session session, Person person)
			throws ServiceFailure, NotAuthorized, IdentifierNotUnique,
			InvalidCredentials, NotImplemented, InvalidRequest, InvalidToken;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.updateAccount">see DataONE API Reference</a> } 
	 */
	public Subject updateAccount(Person person) throws ServiceFailure,
			NotAuthorized, InvalidCredentials, NotImplemented, InvalidRequest,
			InvalidToken, NotFound;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.updateAccount">see DataONE API Reference</a> } 
	 */
	public Subject updateAccount(Session session, Person person)
			throws ServiceFailure, NotAuthorized, InvalidCredentials,
			NotImplemented, InvalidRequest, InvalidToken, NotFound;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.verifyAccount">see DataONE API Reference</a> } 
	 */
	public boolean verifyAccount(Subject subject) throws ServiceFailure,
			NotAuthorized, NotImplemented, InvalidToken, InvalidRequest;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.verifyAccount">see DataONE API Reference</a> } 
	 */
	public boolean verifyAccount(Session session, Subject subject)
			throws ServiceFailure, NotAuthorized, NotImplemented, InvalidToken,
			InvalidRequest;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.getSubjectInfo">see DataONE API Reference</a> } 
	 */
	public SubjectInfo getSubjectInfo(Subject subject) throws ServiceFailure,
			NotAuthorized, NotImplemented, NotFound, InvalidToken;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.getSubjectInfo">see DataONE API Reference</a> } 
	 */
	public SubjectInfo getSubjectInfo(Session session, Subject subject)
			throws ServiceFailure, NotAuthorized, NotImplemented, NotFound,
			InvalidToken;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.listSubjects">see DataONE API Reference</a> } 
	 */
	public SubjectInfo listSubjects(String query, String status, Integer start,
			Integer count) throws InvalidRequest, ServiceFailure, InvalidToken,
			NotAuthorized, NotImplemented;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.listSubjects">see DataONE API Reference</a> } 
	 */
	public SubjectInfo listSubjects(Session session, String query,
			String status, Integer start, Integer count) throws InvalidRequest,
			ServiceFailure, InvalidToken, NotAuthorized, NotImplemented;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.mapIdentity">see DataONE API Reference</a> }
	 */
	public boolean mapIdentity(Subject primarySubject, Subject secondarySubject)
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
			NotImplemented, InvalidRequest, IdentifierNotUnique;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.mapIdentity">see DataONE API Reference</a> }
	 */
	public boolean mapIdentity(Session session, Subject primarySubject,
			Subject secondarySubject) throws ServiceFailure, InvalidToken,
			NotAuthorized, NotFound, NotImplemented, InvalidRequest,
			IdentifierNotUnique;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.requestMapIdentity">see DataONE API Reference</a> } 
	 */
	public boolean requestMapIdentity(Subject subject) throws ServiceFailure,
			InvalidToken, NotAuthorized, NotFound, NotImplemented,
			InvalidRequest, IdentifierNotUnique;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.requestMapIdentity">see DataONE API Reference</a> } 
	 */
	public boolean requestMapIdentity(Session session, Subject subject)
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
			NotImplemented, InvalidRequest, IdentifierNotUnique;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.getPendingMapIdentity">see DataONE API Reference</a> }
	 */
	public SubjectInfo getPendingMapIdentity(Subject subject)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
			NotImplemented;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.getPendingMapIdentity">see DataONE API Reference</a> }
	 */
	public SubjectInfo getPendingMapIdentity(Session session, Subject subject)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
			NotImplemented;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.confirmMapIdentity">see DataONE API Reference</a> } 
	 */
	public boolean confirmMapIdentity(Subject subject) throws ServiceFailure,
			InvalidToken, NotAuthorized, NotFound, NotImplemented;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.confirmMapIdentity">see DataONE API Reference</a> } 
	 */
	public boolean confirmMapIdentity(Session session, Subject subject)
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
			NotImplemented;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.denyMapIdentity">see DataONE API Reference</a> } 
	 */
	public boolean denyMapIdentity(Subject subject) throws ServiceFailure,
			InvalidToken, NotAuthorized, NotFound, NotImplemented;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.denyMapIdentity">see DataONE API Reference</a> } 
	 */
	public boolean denyMapIdentity(Session session, Subject subject)
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
			NotImplemented;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.removeMapIdentity">see DataONE API Reference</a> } 
	 */
	public boolean removeMapIdentity(Subject subject) throws ServiceFailure,
			InvalidToken, NotAuthorized, NotFound, NotImplemented;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.removeMapIdentity">see DataONE API Reference</a> } 
	 */
	public boolean removeMapIdentity(Session session, Subject subject)
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
			NotImplemented;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.createGroup">see DataONE API Reference</a> } 
	 */
	public Subject createGroup(Group group) throws ServiceFailure,
			InvalidToken, NotAuthorized, NotImplemented, IdentifierNotUnique;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.createGroup">see DataONE API Reference</a> } 
	 */
	public Subject createGroup(Session session, Group group)
			throws ServiceFailure, InvalidToken, NotAuthorized, NotImplemented,
			IdentifierNotUnique;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.addGroupMembers">see DataONE API Reference</a> } 
	 */
	public boolean updateGroup(Group group) throws ServiceFailure,
			InvalidToken, NotAuthorized, NotFound, NotImplemented,
			InvalidRequest;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNIdentity.addGroupMembers">see DataONE API Reference</a> } 
	 */
	public boolean updateGroup(Session session, Group group)
			throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
			NotImplemented, InvalidRequest;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRegister.updateNodeCapabilities">see DataONE API Reference</a> } 
	 */
	public boolean updateNodeCapabilities(NodeReference nodeid, Node node)
			throws NotImplemented, NotAuthorized, ServiceFailure,
			InvalidRequest, NotFound, InvalidToken;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRegister.updateNodeCapabilities">see DataONE API Reference</a> } 
	 */
	public boolean updateNodeCapabilities(Session session,
			NodeReference nodeid, Node node) throws NotImplemented,
			NotAuthorized, ServiceFailure, InvalidRequest, NotFound,
			InvalidToken;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRegister.register">see DataONE API Reference</a> }
	 */
	public NodeReference register(Node node) throws NotImplemented,
			NotAuthorized, ServiceFailure, InvalidRequest, IdentifierNotUnique,
			InvalidToken;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRegister.register">see DataONE API Reference</a> }
	 */
	public NodeReference register(Session session, Node node)
			throws NotImplemented, NotAuthorized, ServiceFailure,
			InvalidRequest, IdentifierNotUnique, InvalidToken;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.setReplicationStatus">see DataONE API Reference</a> }
	 */
	public boolean setReplicationStatus(Identifier pid, NodeReference nodeRef,
			ReplicationStatus status, BaseException failure)
			throws ServiceFailure, NotImplemented, InvalidToken, NotAuthorized,
			InvalidRequest, NotFound;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.setReplicationStatus">see DataONE API Reference</a> }
	 */
	public boolean setReplicationStatus(Session session, Identifier pid,
			NodeReference nodeRef, ReplicationStatus status,
			BaseException failure) throws ServiceFailure, NotImplemented,
			InvalidToken, NotAuthorized, InvalidRequest, NotFound;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.setReplicationPolicy">see DataONE API Reference</a> }
	 */
	public boolean setReplicationPolicy(Identifier pid,
			ReplicationPolicy policy, long serialVersion)
			throws NotImplemented, NotFound, NotAuthorized, ServiceFailure,
			InvalidRequest, InvalidToken, VersionMismatch;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.setReplicationPolicy">see DataONE API Reference</a> }
	 */
	public boolean setReplicationPolicy(Session session, Identifier pid,
			ReplicationPolicy policy, long serialVersion)
			throws NotImplemented, NotFound, NotAuthorized, ServiceFailure,
			InvalidRequest, InvalidToken, VersionMismatch;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.isNodeAuthorized">see DataONE API Reference</a> }
	 */
	public boolean isNodeAuthorized(Subject targetNodeSubject, Identifier pid)
			throws NotImplemented, NotAuthorized, InvalidToken, ServiceFailure,
			NotFound, InvalidRequest;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.isNodeAuthorized">see DataONE API Reference</a> }
	 */
	public boolean isNodeAuthorized(Session session, Subject targetNodeSubject,
			Identifier pid) throws NotImplemented, NotAuthorized, InvalidToken,
			ServiceFailure, NotFound, InvalidRequest;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.updateReplicationMetadata">see DataONE API Reference</a> }
	 */
	public boolean updateReplicationMetadata(Identifier pid,
			Replica replicaMetadata, long serialVersion) throws NotImplemented,
			NotAuthorized, ServiceFailure, NotFound, InvalidRequest,
			InvalidToken, VersionMismatch;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.updateReplicationMetadata">see DataONE API Reference</a> }
	 */
	public boolean updateReplicationMetadata(Session session, Identifier pid,
			Replica replicaMetadata, long serialVersion) throws NotImplemented,
			NotAuthorized, ServiceFailure, NotFound, InvalidRequest,
			InvalidToken, VersionMismatch;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.deleteReplicationMetadata">see DataONE API Reference</a> }
	 * @throws InvalidRequest 
	 */
	public boolean deleteReplicationMetadata(Identifier pid,
			NodeReference nodeId, long serialVersion) throws InvalidToken,
			ServiceFailure, NotAuthorized, NotFound, NotImplemented,
			VersionMismatch, InvalidRequest;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNReplication.deleteReplicationMetadata">see DataONE API Reference</a> }
	 * @throws InvalidRequest 
	 */
	public boolean deleteReplicationMetadata(Session session, Identifier pid,
			NodeReference nodeId, long serialVersion) throws InvalidToken,
			ServiceFailure, NotAuthorized, NotFound, NotImplemented,
			VersionMismatch, InvalidRequest;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#CNCore.archive">see DataONE API Reference</a> }
	 */
	public Identifier archive(Identifier pid) throws InvalidToken,
			ServiceFailure, NotAuthorized, NotFound, NotImplemented;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#CNCore.archive.archive">see DataONE API Reference</a> }
	 */
	public Identifier archive(Session session, Identifier pid)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
			NotImplemented;

	public Identifier delete(Identifier pid) throws InvalidToken,
			ServiceFailure, NotAuthorized, NotFound, NotImplemented;

	public Identifier delete(Session session, Identifier pid)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
			NotImplemented;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.query">see DataONE API Reference</a> } 
	 */
	public InputStream query(String queryEngine, String query)
			throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
			NotImplemented, NotFound;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.query">see DataONE API Reference</a> } 
	 */
	public InputStream query(String queryEngine, D1Url queryD1Url)
			throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
			NotImplemented, NotFound;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.getQueryEngineDescription">see DataONE API Reference</a> } 
	 */
	public QueryEngineDescription getQueryEngineDescription(String queryEngine)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented,
			NotFound;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/CN_APIs.html#CNRead.listQueryEngines">see DataONE API Reference</a> } 
	 */
	public QueryEngineList listQueryEngines() throws InvalidToken,
			ServiceFailure, NotAuthorized, NotImplemented;

}