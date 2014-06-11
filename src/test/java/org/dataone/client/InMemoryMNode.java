package org.dataone.client;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.dataone.client.types.D1TypeBuilder;
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
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.DescribeResponse;
import org.dataone.service.types.v1.Event;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Log;
import org.dataone.service.types.v1.LogEntry;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ObjectInfo;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.types.v1.util.ChecksumUtil;
import org.dataone.service.types.v1_1.QueryEngineDescription;
import org.dataone.service.types.v1_1.QueryEngineList;

public class InMemoryMNode implements MNode {
	private NodeReference nodeId;
	
	private Map<Identifier, byte[]> objectStore;
	private Map<Identifier, SystemMetadata> metaStore;
	private List<LogEntry> eventLog;

	private LogEntry buildLogEntry(Event v, Identifier pid, Session session) {
		LogEntry le = new LogEntry();
		le.setDateLogged(new Date());
		le.setEvent(v);
		le.setIdentifier(pid);
		le.setNodeIdentifier(getNodeId());
		le.setSubject(session.getSubject());
		le.setEntryId(String.format("%ddd", eventLog.size()+1));
		return le;
	}
	
	@Override
	public String getNodeBaseServiceUrl() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NodeReference getNodeId() {
		return this.nodeId;
	}

	@Override
	public void setNodeId(NodeReference nodeId) {
		this.nodeId = nodeId;
	}

	@Override
	public void setNodeType(NodeType nodeType) {
		// don't do anything
	}

	@Override
	public NodeType getNodeType() {
		return NodeType.MN;
	}

	@Override
	public Date ping() throws NotImplemented, ServiceFailure,
			InsufficientResources {
		return new Date();
	}

	@Override
	public Log getLogRecords(Date fromDate, Date toDate, Event event,
			String pidFilter, Integer start, Integer count)
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure {
		return getLogRecords(null, fromDate, toDate, event, pidFilter, start, count);
	}

	@Override
	public Node getCapabilities() throws NotImplemented, ServiceFailure {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@Deprecated
	public Log getLogRecords(Session session, Date fromDate, Date toDate,
			Event event, String pidFilter, Integer start, Integer count)
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure {
		// TODO Auto-generated method stub
		return null;
	}

	
	@Override
	public InputStream get(Identifier id) throws InvalidToken, NotAuthorized,
			NotImplemented, ServiceFailure, NotFound, InsufficientResources {
		return get(null, id);
	}

	@Override
	public SystemMetadata getSystemMetadata(Identifier id)
			throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure,
			NotFound {
		return getSystemMetadata(null, id);
	}

	@Override
	public DescribeResponse describe(Identifier id) throws InvalidToken,
			NotAuthorized, NotImplemented, ServiceFailure, NotFound {
		return describe(null, id);
	}

	@Override
	public Checksum getChecksum(Identifier id, String checksumAlgorithm)
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure, NotFound {
		return getChecksum(null, id, checksumAlgorithm);
	}

	/**
	 * 
	 */
	@Override
	public ObjectList listObjects(Date fromDate, Date toDate,
			ObjectFormatIdentifier formatid, Boolean replicaStatus,
			Integer start, Integer count) throws InvalidRequest, InvalidToken,
			NotAuthorized, NotImplemented, ServiceFailure {
		
		return listObjects(null, fromDate, toDate, formatid, replicaStatus, start, count);
	}

	@Override
	public boolean synchronizationFailed(SynchronizationFailed message)
			throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure {
		return synchronizationFailed(null, message);
	}

	@Override
	public InputStream getReplica(Identifier pid) throws InvalidToken,
			NotAuthorized, NotImplemented, ServiceFailure, NotFound,
			InsufficientResources {
		return getReplica(null, pid);
	}

	@Override
	@Deprecated
	public InputStream get(Session session, Identifier id)
			throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure,
			NotFound, InsufficientResources {
		
		InputStream is = getReplica(session, id);
		eventLog.add(buildLogEntry(Event.READ, id, session));
		return is;
	}


	
	@Override
	@Deprecated
	public SystemMetadata getSystemMetadata(Session session, Identifier id)
			throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure,
			NotFound {
		if (metaStore.containsKey(id)) 
			return metaStore.get(id);
		
		throw new NotFound("000",
				String.format("Sysmeta for id '%s' could not be found", 
				id.getValue())
				);
	}

	@Override
	@Deprecated
	public DescribeResponse describe(Session session, Identifier id)
			throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure,
			NotFound {
		if (metaStore.containsKey(id)) {
			SystemMetadata sysmeta =  metaStore.get(id);
			return new DescribeResponse(
					sysmeta.getFormatId(),
					sysmeta.getSize(),
					sysmeta.getDateSysMetadataModified(),
					sysmeta.getChecksum(),
					sysmeta.getSerialVersion());
		}
		
		throw new NotFound("000",
				String.format("Sysmeta for id '%s' could not be found", 
				id.getValue())
				);
	}

	@Override
	@Deprecated
	public Checksum getChecksum(Session session, Identifier id,
			String checksumAlgorithm) throws InvalidRequest, InvalidToken,
			NotAuthorized, NotImplemented, ServiceFailure, NotFound {
		
		if (objectStore.containsKey(id)) {
			try {
				return ChecksumUtil.checksum(objectStore.get(id), checksumAlgorithm);
			} catch (NoSuchAlgorithmException e) {
				throw new InvalidRequest("000", "Could not calculate checksum using" +
						"the provided algorithm (" + checksumAlgorithm +")");
			}
		}
		
		throw new NotFound("000",
				String.format("object for id '%s' could not be found", 
				id.getValue())
				);
			
	}

	@Override
	@Deprecated
	public ObjectList listObjects(Session session, Date fromDate, Date toDate,
			ObjectFormatIdentifier formatid, Boolean replicaStatus,
			Integer start, Integer count) throws InvalidRequest, InvalidToken,
			NotAuthorized, NotImplemented, ServiceFailure {
		ObjectList ol = new ObjectList();
		TreeMap<String, ObjectInfo> oiMap = new TreeMap<String,ObjectInfo>();
		for(Entry<Identifier, SystemMetadata> en : metaStore.entrySet()) {
			SystemMetadata s = en.getValue();
			if (fromDate != null && s.getDateSysMetadataModified().before(fromDate))
				continue;
			if (toDate != null && s.getDateSysMetadataModified().after(toDate))
				continue;
			if (toDate != null && s.getDateSysMetadataModified().equals(toDate))
				continue;
			if (formatid != null && !s.getFormatId().equals(formatid)) 
				continue;
			
			// survived the filters
			ObjectInfo oi = new ObjectInfo();
			oi.setChecksum(s.getChecksum());
			oi.setDateSysMetadataModified(s.getDateSysMetadataModified());
			oi.setFormatId(s.getFormatId());
			oi.setIdentifier(s.getIdentifier());
			oi.setSize(s.getSize());
			oiMap.put(en.getKey().getValue(), oi);
		}

		if (!oiMap.isEmpty()) {
			if (count == null || count != 0) {
				Iterator<String> it = oiMap.keySet().iterator();
				int first = start == null ? 0 : start;
				int i = -1;
				while (i + 1 < first) {
					it.next();
					i++;
				}
				int c = 0;
				while (it.hasNext() && count > c) {
					String s = it.next();
					ol.addObjectInfo(oiMap.get(s));
					c++;
				}
			}
		}
		return ol; 
	}

	@Override
	@Deprecated
	public boolean synchronizationFailed(Session session,
			SynchronizationFailed message) throws InvalidToken, NotAuthorized,
			NotImplemented, ServiceFailure {
		
		eventLog.add(buildLogEntry(Event.SYNCHRONIZATION_FAILED, 
					D1TypeBuilder.buildIdentifier(message.getPid()), session));
		
		return true;
	}

	@Override
	@Deprecated
	public InputStream getReplica(Session session, Identifier id)
			throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure,
			NotFound, InsufficientResources {
		// TODO handle SIDs??
		if (objectStore.containsKey(id)) 
			return new ByteArrayInputStream(objectStore.get(id));

		throw new NotFound("000",
				String.format("Object with id '%s' could not be found", 
						id.getValue())
				);
	}

	@Override
	public boolean isAuthorized(Identifier pid, Permission action)
			throws ServiceFailure, InvalidRequest, InvalidToken, NotFound,
			NotAuthorized, NotImplemented {
		return isAuthorized(null,pid,action);
	}

	@Override
	public boolean systemMetadataChanged(Identifier pid, long serialVersion,
			Date dateSystemMetadataLastModified) throws InvalidToken,
			ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest {

		return systemMetadataChanged(null,pid,serialVersion, dateSystemMetadataLastModified);
	}

	@Override
	@Deprecated
	public boolean isAuthorized(Session session, Identifier pid,
			Permission action) throws ServiceFailure, InvalidRequest,
			InvalidToken, NotFound, NotAuthorized, NotImplemented {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	@Deprecated
	public boolean systemMetadataChanged(Session session, Identifier pid,
			long serialVersion, Date dateSystemMetadataLastModified)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented,
			InvalidRequest {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Identifier create(Identifier pid, InputStream object,
			SystemMetadata sysmeta) throws IdentifierNotUnique,
			InsufficientResources, InvalidRequest, InvalidSystemMetadata,
			InvalidToken, NotAuthorized, NotImplemented, ServiceFailure,
			UnsupportedType {
		return create(null, pid, object, sysmeta);
	}

	@Override
	public Identifier update(Identifier pid, InputStream object,
			Identifier newPid, SystemMetadata sysmeta)
			throws IdentifierNotUnique, InsufficientResources, InvalidRequest,
			InvalidSystemMetadata, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure, UnsupportedType, NotFound 
	{
		return update(null, pid, object, newPid, sysmeta);
	}

	@Override
	public Identifier delete(Identifier pid) throws InvalidToken,
			ServiceFailure, NotAuthorized, NotFound, NotImplemented {
		return delete(null, pid);
	}

	@Override
	public Identifier archive(Identifier pid) throws InvalidToken,
			ServiceFailure, NotAuthorized, NotFound, NotImplemented 
	{
		return archive(null,pid);
	}

	@Override
	public Identifier generateIdentifier(String scheme, String fragment)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented,
			InvalidRequest 
	{
		return generateIdentifier(null,scheme, fragment);
	}

	@Override
	@Deprecated
	public Identifier create(Session session, Identifier pid,
			InputStream object, SystemMetadata sysmeta)
			throws IdentifierNotUnique, InsufficientResources, InvalidRequest,
			InvalidSystemMetadata, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure, UnsupportedType 
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@Deprecated
	public Identifier update(Session session, Identifier pid,
			InputStream object, Identifier newPid, SystemMetadata sysmeta)
			throws IdentifierNotUnique, InsufficientResources, InvalidRequest,
			InvalidSystemMetadata, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure, UnsupportedType, NotFound {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@Deprecated
	public Identifier delete(Session session, Identifier pid)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
			NotImplemented {
		try {
			if (isAuthorized(session, pid, Permission.CHANGE_PERMISSION)) {
				objectStore.remove(pid);
				eventLog.add(buildLogEntry(Event.DELETE, pid, session));
			}
		} catch (InvalidRequest e) {
			throw new InvalidToken("000","User doesn't have authorization to delete");
		}
		return null;
	}

	@Override
	@Deprecated
	public Identifier archive(Session session, Identifier pid)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
			NotImplemented {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@Deprecated
	public Identifier generateIdentifier(Session session, String scheme,
			String fragment) throws InvalidToken, ServiceFailure,
			NotAuthorized, NotImplemented, InvalidRequest {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean replicate(SystemMetadata sysmeta, NodeReference sourceNode)
			throws NotImplemented, ServiceFailure, NotAuthorized,
			InvalidRequest, InvalidToken, InsufficientResources,
			UnsupportedType {
		return replicate(null, sysmeta, sourceNode);
	}

	@Override
	@Deprecated
	public boolean replicate(Session session, SystemMetadata sysmeta,
			NodeReference sourceNode) throws NotImplemented, ServiceFailure,
			NotAuthorized, InvalidRequest, InvalidToken, InsufficientResources,
			UnsupportedType {
		// TODO Auto-generated method stub
		return false;
	}

	
	
	@Override
	public InputStream query(String queryEngine, String query)
			throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
			NotImplemented, NotFound {
		return null;
	}

	@Override
	public QueryEngineDescription getQueryEngineDescription(String queryEngine)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented,
			NotFound {
		return null;
	}

	@Override
	public QueryEngineList listQueryEngines() throws InvalidToken,
			ServiceFailure, NotAuthorized, NotImplemented {
		return null;
	}

}
