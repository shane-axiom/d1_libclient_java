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

import java.io.InputStream;
import java.util.Date;

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
import org.dataone.service.mn.v1.MNQuery;
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
import org.dataone.service.types.v1_1.QueryEngineDescription;
import org.dataone.service.types.v1_1.QueryEngineList;

public interface MNode 
extends MNCore, MNRead, MNAuthorization, MNStorage, MNReplication, MNQuery 
{

	public String getNodeBaseServiceUrl();

	/**
     * @return the nodeId
     */
    public String getNodeId();

    /**
     * @param nodeId the nodeId to set
     */
    public void setNodeId(String nodeId);
    
    
	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNCore.ping">see DataONE API Reference</a> } 
	 */
	public Date ping() throws NotImplemented, ServiceFailure,
			InsufficientResources;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNCore.getLogRecords">see DataONE API Reference</a> }
	 */
	public Log getLogRecords() throws InvalidRequest, InvalidToken,
			NotAuthorized, NotImplemented, ServiceFailure;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNCore.getLogRecords">see DataONE API Reference</a> }
	 */
	public Log getLogRecords(Session session) throws InvalidRequest,
			InvalidToken, NotAuthorized, NotImplemented, ServiceFailure;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNCore.getLogRecords">see DataONE API Reference</a> }
	 * 
	 */
	public Log getLogRecords(Date fromDate, Date toDate, Event event,
			String pidFilter, Integer start, Integer count)
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNCore.getLogRecords">see DataONE API Reference</a> }
	 * 
	 */
	public Log getLogRecords(Session session, Date fromDate, Date toDate,
			Event event, String pidFilter, Integer start, Integer count)
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.listObjects">see DataONE API Reference</a> }
	 */
	public ObjectList listObjects() throws InvalidRequest, InvalidToken,
			NotAuthorized, NotImplemented, ServiceFailure;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.listObjects">see DataONE API Reference</a> }
	 */
	public ObjectList listObjects(Session session) throws InvalidRequest,
			InvalidToken, NotAuthorized, NotImplemented, ServiceFailure;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.listObjects">see DataONE API Reference</a> }
	 */
	public ObjectList listObjects(Date fromDate, Date toDate,
			ObjectFormatIdentifier formatid, Boolean replicaStatus,
			Integer start, Integer count) throws InvalidRequest, InvalidToken,
			NotAuthorized, NotImplemented, ServiceFailure;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.listObjects">see DataONE API Reference</a> }
	 */
	public ObjectList listObjects(Session session, Date fromDate, Date toDate,
			ObjectFormatIdentifier formatid, Boolean replicaStatus,
			Integer start, Integer count) throws InvalidRequest, InvalidToken,
			NotAuthorized, NotImplemented, ServiceFailure;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNCore.getCapabilities">see DataONE API Reference</a> }
	 */
	public Node getCapabilities() throws NotImplemented, ServiceFailure;

	public InputStream get(Identifier pid) throws InvalidToken, NotAuthorized,
			NotImplemented, ServiceFailure, NotFound, InsufficientResources;

	public InputStream get(Session session, Identifier pid)
			throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure,
			NotFound, InsufficientResources;

	public SystemMetadata getSystemMetadata(Identifier pid)
			throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure,
			NotFound;

	public SystemMetadata getSystemMetadata(Session session, Identifier pid)
			throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure,
			NotFound;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.describe">see DataONE API Reference</a> } 
	 */
	public DescribeResponse describe(Identifier pid) throws InvalidToken,
			NotAuthorized, NotImplemented, ServiceFailure, NotFound;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.describe">see DataONE API Reference</a> } 
	 */
	public DescribeResponse describe(Session session, Identifier pid)
			throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure,
			NotFound;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.getChecksum">see DataONE API Reference</a> } 
	 */
	public Checksum getChecksum(Identifier pid, String checksumAlgorithm)
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure, NotFound;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.getChecksum">see DataONE API Reference</a> } 
	 */
	public Checksum getChecksum(Session session, Identifier pid,
			String checksumAlgorithm) throws InvalidRequest, InvalidToken,
			NotAuthorized, NotImplemented, ServiceFailure, NotFound;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.synchronizationFailed">see DataONE API Reference</a> } 
	 */
	public boolean synchronizationFailed(SynchronizationFailed message)
			throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.synchronizationFailed">see DataONE API Reference</a> } 
	 */
	public boolean synchronizationFailed(Session session,
			SynchronizationFailed message) throws InvalidToken, NotAuthorized,
			NotImplemented, ServiceFailure;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNAuthorization.isAuthorized">see DataONE API Reference</a> } 
	 */
	public boolean isAuthorized(Identifier pid, Permission action)
			throws ServiceFailure, InvalidRequest, InvalidToken, NotFound,
			NotAuthorized, NotImplemented;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNAuthorization.isAuthorized">see DataONE API Reference</a> } 
	 */
	public boolean isAuthorized(Session session, Identifier pid,
			Permission action) throws ServiceFailure, InvalidRequest,
			InvalidToken, NotFound, NotAuthorized, NotImplemented;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.generateIdentifier">see DataONE API Reference</a> } 
	 */
	public Identifier generateIdentifier(String scheme, String fragment)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented,
			InvalidRequest;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.generateIdentifier">see DataONE API Reference</a> } 
	 */
	public Identifier generateIdentifier(Session session, String scheme,
			String fragment) throws InvalidToken, ServiceFailure,
			NotAuthorized, NotImplemented, InvalidRequest;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.create">see DataONE API Reference</a> } 
	 */
	public Identifier create(Identifier pid, InputStream object,
			SystemMetadata sysmeta) throws IdentifierNotUnique,
			InsufficientResources, InvalidRequest, InvalidSystemMetadata,
			InvalidToken, NotAuthorized, NotImplemented, ServiceFailure,
			UnsupportedType;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.create">see DataONE API Reference</a> } 
	 */
	public Identifier create(Session session, Identifier pid,
			InputStream object, SystemMetadata sysmeta)
			throws IdentifierNotUnique, InsufficientResources, InvalidRequest,
			InvalidSystemMetadata, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure, UnsupportedType;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.update">see DataONE API Reference</a> } 
	 */
	public Identifier update(Identifier pid, InputStream object,
			Identifier newPid, SystemMetadata sysmeta)
			throws IdentifierNotUnique, InsufficientResources, InvalidRequest,
			InvalidSystemMetadata, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure, UnsupportedType, NotFound;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.update">see DataONE API Reference</a> } 
	 */
	public Identifier update(Session session, Identifier pid,
			InputStream object, Identifier newPid, SystemMetadata sysmeta)
			throws IdentifierNotUnique, InsufficientResources, InvalidRequest,
			InvalidSystemMetadata, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure, UnsupportedType, NotFound;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.archive">see DataONE API Reference</a> }
	 */
	public Identifier archive(Identifier pid) throws InvalidToken,
			ServiceFailure, NotAuthorized, NotFound, NotImplemented;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.archive">see DataONE API Reference</a> }
	 */
	public Identifier archive(Session session, Identifier pid)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
			NotImplemented;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.delete">see DataONE API Reference</a> }
	 */
	public Identifier delete(Identifier pid) throws InvalidToken,
			ServiceFailure, NotAuthorized, NotFound, NotImplemented;

	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.delete">see DataONE API Reference</a> }
	 */
	public Identifier delete(Session session, Identifier pid)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
			NotImplemented;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNAuthorization.systemMetadataChanged">see DataONE API Reference</a> } 
	 */
	public boolean systemMetadataChanged(Identifier pid, long serialVersion,
			Date dateSystemMetadataLastModified) throws InvalidToken,
			ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNAuthorization.systemMetadataChanged">see DataONE API Reference</a> } 
	 */
	public boolean systemMetadataChanged(Session session, Identifier pid,
			long serialVersion, Date dateSystemMetadataLastModified)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented,
			InvalidRequest;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNReplication.replicate">see DataONE API Reference</a> } 
	 */
	public boolean replicate(SystemMetadata sysmeta, NodeReference sourceNode)
			throws NotImplemented, ServiceFailure, NotAuthorized,
			InvalidRequest, InvalidToken, InsufficientResources,
			UnsupportedType;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNReplication.replicate">see DataONE API Reference</a> } 
	 */
	public boolean replicate(Session session, SystemMetadata sysmeta,
			NodeReference sourceNode) throws NotImplemented, ServiceFailure,
			NotAuthorized, InvalidRequest, InvalidToken, InsufficientResources,
			UnsupportedType;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.getReplica">see DataONE API Reference</a> } 
	 */
	public InputStream getReplica(Identifier pid) throws InvalidToken,
			NotAuthorized, NotImplemented, ServiceFailure, NotFound,
			InsufficientResources;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.getReplica">see DataONE API Reference</a> } 
	 */
	public InputStream getReplica(Session session, Identifier pid)
			throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure,
			NotFound, InsufficientResources;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNQuery.query">see DataONE API Reference</a> } 
	 */
	public InputStream query(String queryEngine, String query)
			throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
			NotImplemented, NotFound;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNQuery.getQueryEngineDescription">see DataONE API Reference</a> } 
	 */
	public QueryEngineDescription getQueryEngineDescription(String queryEngine)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented,
			NotFound;

	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNQuery.listQueryEngines">see DataONE API Reference</a> } 
	 */
	public QueryEngineList listQueryEngines() throws InvalidToken,
			ServiceFailure, NotAuthorized, NotImplemented;

}