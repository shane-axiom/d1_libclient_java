package org.dataone.client.v1.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.dataone.client.v1.MNode;
import org.dataone.client.v1.types.D1TypeBuilder;
import org.dataone.client.v2.formats.ObjectFormatCache;
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
import org.dataone.service.types.v1.ObjectFormat;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ObjectInfo;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.types.v1.util.AuthUtils;
import org.dataone.service.types.v1.util.ChecksumUtil;
import org.dataone.service.types.v1_1.QueryEngineDescription;
import org.dataone.service.types.v1_1.QueryEngineList;
import org.dataone.service.util.TypeMarshaller;
import org.jibx.runtime.JiBXException;

/**
 * Built primarily for testing, this class is an MNode implementation that 
 * holds all of its content as SystemMetadata, LogEntries, and byte arrays (for 
 * data objects)
 * 
 * All methods return either the expected type or throw a NotImplemented exception.
 * Users should be cautious about object size.
 * 
 * @author rnahf
 *
 */
public class InMemoryMNode implements MNode {
	private NodeReference nodeId;
	
	protected Map<Identifier, byte[]> objectStore;
	protected Map<Identifier, SystemMetadata> metaStore;
	protected List<LogEntry> eventLog;
	protected Subject nodeAdministrator;
	protected Subject cnClientUser;
	
	/** 
	 * Instantiate a new InMemberMemberNode.  If the cnClientSubject is null, then
	 * getLogRecords will authorize anyone.
	 * 
	 * @param nodeAdmin - the Subject of this MemberNode's administrator
	 * @param cnClientSubject - the Subject of the CN.
	 */
	public InMemoryMNode(Subject nodeAdmin, Subject cnClientSubject) {
		this.nodeAdministrator = nodeAdmin;
		this.cnClientUser = cnClientSubject;
		this.objectStore = new HashMap<Identifier, byte[]>();
		this.metaStore = new HashMap<Identifier,SystemMetadata>();
		this.eventLog = new ArrayList<LogEntry>();
	}
	
	protected synchronized LogEntry buildLogEntry(Event v, Identifier pid, Session session) {
		LogEntry le = new LogEntry();
		le.setDateLogged(new Date());
		le.setEvent(v);
		le.setIdentifier(pid);
		le.setNodeIdentifier(getNodeId());
		if (session != null)
		    le.setSubject(session.getSubject());
		le.setEntryId(String.format("%ddd", eventLog.size()+1));
		return le;
	}
	
	private SystemMetadata checkAvailableAndAuthorized(Session session, Identifier id, Permission perm)
	throws NotAuthorized, NotFound
	{
		SystemMetadata sysmeta = metaStore.get(id);
		if (sysmeta == null) {
			throw new NotFound("000",
					String.format("Object with id %s could not be found", 
							id.getValue())
					);
		}
		Set<Subject> sessionSubjects = AuthUtils.authorizedClientSubjects(session);
		if (sessionSubjects.contains(nodeAdministrator) ||
		        sessionSubjects.contains(cnClientUser)) {
		    return sysmeta;
		}
		if (!AuthUtils.isAuthorized(sessionSubjects, perm, sysmeta)) {
			throw new NotAuthorized("000",String.format("Caller (%s) does not have %s" +
					" permission on %s",
					joinSubjects(sessionSubjects),
					perm.xmlValue(),
					sysmeta.getIdentifier().getValue()));
		}
		return sysmeta;
	}
	
	private String joinSubjects(Set<Subject> subs) {
	    StringBuffer sb = new StringBuffer();
	    for (Subject s : subs) {
            sb.append(",");
	        sb.append(s.getValue());
	    }
	    return sb.toString().substring(1);
	}
	
	
	/**
	 * Validate that the systemMetadata follows the D1_Schema definitions
	 * Doing it through serialization and deserialization (probably a bit overkill)
	 * 
	 * @param sysmeta
	 * @throws InvalidSystemMetadata
	 */
	private void validateSystemMetadata(SystemMetadata sysmeta) 
	throws InvalidSystemMetadata 
	{	
		Exception caught = null;
		try {
			ByteArrayOutputStream os = new ByteArrayOutputStream(512);
			TypeMarshaller.marshalTypeToOutputStream(sysmeta, os);
			os.close();
			// maybe we don't need to reconstitute to validate...
			TypeMarshaller.unmarshalTypeFromStream(SystemMetadata.class, 
					new ByteArrayInputStream(os.toByteArray()) );
		} catch (JiBXException e) {
			caught = e;
		} catch (IOException e) {
			caught = e;
		} catch (InstantiationException e) {
			caught = e;
		} catch (IllegalAccessException e) {
			caught = e;
		}
		if (caught != null) {
			InvalidSystemMetadata be = new InvalidSystemMetadata("000","The SystemMetadata is invalid");
			be.initCause(caught);
			throw be;
		}
	}
	
	/**
	 * We will use the NodeId value for the base service URL, too.
	 */
	@Override
	public String getNodeBaseServiceUrl() {
		
		return this.nodeId.getValue();
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
	public String getLatestRequestUrl() {
		return "No request info available";
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
		throw new NotImplemented("000","getCapabilities is not implemented.");
	}

	@Override
	@Deprecated
	public Log getLogRecords(Session session, Date fromDate, Date toDate,
			Event event, String pidFilter, Integer start, Integer count)
	throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure 
	{
		// restrict access to the CN client user subject
		if (this.cnClientUser != null) {
			Set<Subject> requestSubjects = AuthUtils.authorizedClientSubjects(session);
		
			if (!requestSubjects.contains(this.cnClientUser)) {
				throw new NotAuthorized("000", "The requestor's session does not contain" +
						"the known CN Client Subject: " + this.cnClientUser.getValue()); 
			}
		}
		
		Log result = new Log();
		List<LogEntry> filteredLogs = new ArrayList<LogEntry>();
			
		for(LogEntry en : eventLog) {
			if (event != null && !en.getEvent().equals(event))
				continue;
			if (fromDate != null && en.getDateLogged().before(fromDate))
				continue;
			if (toDate != null && en.getDateLogged().after(toDate))
				continue;
			if (toDate != null && en.getDateLogged().equals(toDate))
				continue;
			if (pidFilter != null && !en.getIdentifier().getValue().startsWith(pidFilter))
				continue;
			
			// survived the filters
			filteredLogs.add(en);
		}
		
		int fromIndex = start == null ? 0 : start;
				
		int toIndex = filteredLogs.size();
		if (count != null && (start + count < filteredLogs.size())) {
			toIndex = start + count;
		}
		// note: to subList the entire list, use l.subList(0,l.size())
		for (LogEntry le : filteredLogs.subList(fromIndex, toIndex)) {
			result.addLogEntry(le);
		}
		result.setStart(fromIndex);
		result.setTotal(filteredLogs.size());
		return result;
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
			NotFound 
	{
		return checkAvailableAndAuthorized(session, id, Permission.READ);
	}

	
	@Override
	@Deprecated
	public DescribeResponse describe(Session session, Identifier id)
	throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound 
	{
		SystemMetadata sysmeta =  checkAvailableAndAuthorized(session, id, Permission.READ);
		return new DescribeResponse(
					sysmeta.getFormatId(),
					sysmeta.getSize(),
					sysmeta.getDateSysMetadataModified(),
					sysmeta.getChecksum(),
					sysmeta.getSerialVersion());
			
	}

	/**
	 * This method calculates the checksum afresh every call;
	 */
	@Override
	@Deprecated
	public Checksum getChecksum(Session session, Identifier id,
			String checksumAlgorithm) throws InvalidRequest, InvalidToken,
			NotAuthorized, NotImplemented, ServiceFailure, NotFound {
		
		checkAvailableAndAuthorized(session, id, Permission.READ);
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
			Integer start, Integer count) 
	throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure 
	{
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
		checkAvailableAndAuthorized(session, id, Permission.READ);
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

		checkAvailableAndAuthorized(session, pid, action);
		return true;
	}

	@Override
	@Deprecated
	public boolean systemMetadataChanged(Session session, Identifier pid,
			long serialVersion, Date dateSystemMetadataLastModified)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented,
			InvalidRequest {
		//TODO implement
		throw new NotImplemented("000","systemMetadataChanged is not implemented.");
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
	public Identifier create(Session session, Identifier pid, InputStream object, SystemMetadata sysmeta)
	throws IdentifierNotUnique, InsufficientResources, InvalidRequest,
			InvalidSystemMetadata, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure, UnsupportedType 
	{
		try {
			checkAvailableAndAuthorized(session, pid, Permission.READ);
			// not good
			throw new IdentifierNotUnique("000", pid.getValue() +
					": An object with this identifier already exists.");
		} catch (NotFound nf) {
			// good
			byte[] objectBytes;
			try {
				objectBytes = IOUtils.toByteArray(object);
				this.objectStore.put(pid,objectBytes);
				sysmeta.setDateUploaded(new Date());
				sysmeta.setOriginMemberNode(getNodeId());
				validateSystemMetadata(sysmeta);
				this.metaStore.put(pid, sysmeta);
				eventLog.add(buildLogEntry(Event.CREATE, pid, session));
				
			} catch (IOException e) {
				ServiceFailure sf = new ServiceFailure("000",pid.getValue() +
						"Problem in create() converting the inputStream to byte[].");
				sf.initCause(e);
				throw sf;
			}
			
		} catch (NotAuthorized na) {
			// not good
			throw new IdentifierNotUnique("000", pid.getValue() + 
					": An object with this identifier already exists.");
		}
		
		return pid;
	}

	@Override
	@Deprecated
	public Identifier update(Session session, Identifier pid,
			InputStream object, Identifier newPid, SystemMetadata sysmeta)
	throws IdentifierNotUnique, InsufficientResources, InvalidRequest,
			InvalidSystemMetadata, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure, UnsupportedType, NotFound 
			{
		
		// throws exception if the pid being update (obsoleted) can't be found
		// or the requester doesn't have the permissions to do the update
		checkAvailableAndAuthorized(session, pid, Permission.CHANGE_PERMISSION);			
		
		
		// rule out existence of newPid
		try {
			checkAvailableAndAuthorized(session, newPid, Permission.READ);
			
			// newPid was found...
			throw new IdentifierNotUnique("000", pid.getValue() +
					": An object with this identifier already exists.");
		} 
		catch (NotAuthorized na) {
			throw new IdentifierNotUnique("000", pid.getValue() +
				": An object with this identifier already exists.");
		}  
		catch (NotFound nf) {
		
			byte[] objectBytes;
			try {
				objectBytes = IOUtils.toByteArray(object);
				this.objectStore.put(newPid,objectBytes);
				sysmeta.setDateUploaded(new Date());
				sysmeta.setOriginMemberNode(getNodeId());
				sysmeta.setAuthoritativeMemberNode(getNodeId());
				sysmeta.setDateSysMetadataModified(new Date());
				sysmeta.setObsoletes(pid);
				ObjectFormat of = ObjectFormatCache.getInstance().getFormat(sysmeta.getFormatId());
				if (of == null) {
					throw new UnsupportedType("000","Cannot store data of the format " +
							sysmeta.getFormatId().getValue());
				}
				validateSystemMetadata(sysmeta);

				SystemMetadata oldSysMeta = metaStore.get(pid);
				if (oldSysMeta.getArchived()) {
					throw new InvalidRequest("000","Cannot update an archived object. pid = " 
							+ pid.getValue());
				}
				oldSysMeta.setObsoletedBy(newPid);
				oldSysMeta.setDateSysMetadataModified(new Date());

				this.metaStore.put(newPid, sysmeta);
				eventLog.add(buildLogEntry(Event.UPDATE, pid, session));

			} catch (IOException e) {
				ServiceFailure sf = new ServiceFailure("000",pid.getValue() +
						"Problem in update() converting the inputStream to byte[].");
				sf.initCause(e);
				throw sf;
			}
		}
		return pid;
	}

	
	@Override
	@Deprecated
	public Identifier delete(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented 
	{
		SystemMetadata smd = checkAvailableAndAuthorized(session, pid, Permission.CHANGE_PERMISSION);
		// TODO: what is the semantics of archived, here?  
		archive(session, pid);
		// remove the object 
		objectStore.remove(pid);
		eventLog.add(buildLogEntry(Event.DELETE, pid, session));
		return pid;
	}

	@Override
	@Deprecated
	public Identifier archive(Session session, Identifier pid)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
			NotImplemented 
	{
		SystemMetadata sysmeta = 
				checkAvailableAndAuthorized(session, pid, Permission.CHANGE_PERMISSION);
		sysmeta.setArchived(true);
		sysmeta.setDateSysMetadataModified(new Date());
		
		return pid;
	}

	@Override
	@Deprecated
	public Identifier generateIdentifier(Session session, String scheme,
			String fragment) throws InvalidToken, ServiceFailure,
			NotAuthorized, NotImplemented, InvalidRequest {
		//TODO implement
		throw new NotImplemented("000","generateIdentifier is not implemented.");
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
		//TODO implement
		throw new NotImplemented("000","replicate is not implemented.");
	}

	
	
	@Override
	public InputStream query(String queryEngine, String query)
			throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
			NotImplemented, NotFound {
		//TODO implement
		throw new NotImplemented("000","query is not implemented.");
	}

	@Override
	public QueryEngineDescription getQueryEngineDescription(String queryEngine)
			throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented,
			NotFound {
		//TODO implement
		throw new NotImplemented("000","getQueryEngineDescription is not implemented.");
	}

	@Override
	public QueryEngineList listQueryEngines() throws InvalidToken,
			ServiceFailure, NotAuthorized, NotImplemented {
		//TODO implement
		throw new NotImplemented("000","listQueryEngines is not implemented.");
	}
}
