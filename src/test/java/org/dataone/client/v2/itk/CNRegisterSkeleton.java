package org.dataone.client.v2.itk;

import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.dataone.client.v2.CNode;
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
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.exceptions.VersionMismatch;
import org.dataone.service.types.v1.AccessPolicy;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.ChecksumAlgorithmList;
import org.dataone.service.types.v1.DescribeResponse;
import org.dataone.service.types.v1.Group;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
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
import org.dataone.service.types.v1_1.QueryEngineDescription;
import org.dataone.service.types.v1_1.QueryEngineList;
import org.dataone.service.types.v2.Log;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.NodeList;
import org.dataone.service.types.v2.ObjectFormat;
import org.dataone.service.types.v2.ObjectFormatList;
import org.dataone.service.types.v2.OptionList;
import org.dataone.service.types.v2.SystemMetadata;

public class CNRegisterSkeleton implements CNode {

    private NodeList nodeList = new NodeList();
    private int nextIndex = 0;
    private Map<NodeReference,Integer> nodeIndexMap = new HashMap<>();
    private NodeReference nodeId;
    private String baseUrl;

    public CNRegisterSkeleton(NodeReference nodeId, String baseUrl) {
        this.nodeId = nodeId;
        this.baseUrl = baseUrl;
    }    
    
    
    @Override
    public String getNodeBaseServiceUrl() {
        return baseUrl;
    }
    
    public void setNodeBaseServiceUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    @Override
    public NodeReference getNodeId() {
        return nodeId;
    }

    @Override
    public boolean updateNodeCapabilities(Session session,
            NodeReference nodeid, Node node) throws NotImplemented,
            NotAuthorized, ServiceFailure, InvalidRequest, NotFound,
            InvalidToken {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public NodeReference register(Session session, Node node)
            throws NotImplemented, NotAuthorized, ServiceFailure,
            InvalidRequest, InvalidToken, IdentifierNotUnique {
        // adds another node to the NodeList
        if (!nodeIndexMap.containsKey(node.getIdentifier())) {
            nodeList.addNode(node);
            nodeIndexMap.put(node.getIdentifier(), this.nextIndex++);
            return node.getIdentifier();
        } else {
            throw new InvalidRequest("00", "A node with this identifier is already registered!");
        }
        
    }

    @Override
    public Node getNodeCapabilities(NodeReference nodeid)
            throws NotImplemented, ServiceFailure, InvalidRequest, NotFound {
        // TODO Auto-generated method stub
        if (nodeIndexMap.containsKey(nodeid)) {
            return nodeList.getNode(nodeIndexMap.get(nodeid));
        } else {
            throw new NotFound("00,", "Node not found");
        }
    }
    
    @Override
    public NodeList listNodes() throws NotImplemented, ServiceFailure {
        // TODO Auto-generated method stub
        return this.nodeList;
    }

////////////  the rest of the methods are no-ops  //////////////    
    
    @Override
    public Date ping() throws NotImplemented, ServiceFailure,
            InsufficientResources {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ObjectFormatList listFormats() throws ServiceFailure, NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ObjectFormat getFormat(ObjectFormatIdentifier formatid)
            throws ServiceFailure, NotFound, NotImplemented, InvalidRequest {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ObjectFormatIdentifier addFormat(Session session,
            ObjectFormatIdentifier formatid, ObjectFormat format)
            throws ServiceFailure, NotFound, NotImplemented, InvalidRequest,
            NotAuthorized, InvalidToken {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ChecksumAlgorithmList listChecksumAlgorithms()
            throws ServiceFailure, NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Log getLogRecords(Session session, Date fromDate, Date toDate,
            String event, String pidFilter, Integer start, Integer count)
            throws InvalidToken, InvalidRequest, ServiceFailure, NotAuthorized,
            NotImplemented, InsufficientResources {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Identifier reserveIdentifier(Session session, Identifier id)
            throws InvalidToken, ServiceFailure, NotAuthorized,
            IdentifierNotUnique, NotImplemented, InvalidRequest {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Node getCapabilities() throws NotImplemented, ServiceFailure {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Identifier generateIdentifier(Session session, String scheme,
            String fragment) throws InvalidToken, ServiceFailure,
            NotAuthorized, NotImplemented, InvalidRequest {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean hasReservation(Session session, Subject subject,
            Identifier id) throws InvalidToken, ServiceFailure, NotFound,
            NotAuthorized, NotImplemented, InvalidRequest {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Identifier create(Session session, Identifier pid,
            InputStream object, SystemMetadata sysmeta) throws InvalidToken,
            ServiceFailure, NotAuthorized, IdentifierNotUnique,
            UnsupportedType, InsufficientResources, InvalidSystemMetadata,
            NotImplemented, InvalidRequest {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Identifier registerSystemMetadata(Session session, Identifier pid,
            SystemMetadata sysmeta) throws NotImplemented, NotAuthorized,
            ServiceFailure, InvalidRequest, InvalidSystemMetadata, InvalidToken {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean synchronize(Session session, Identifier pid)
            throws NotImplemented, NotAuthorized, ServiceFailure,
            InvalidRequest, InvalidSystemMetadata, InvalidToken {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean updateSystemMetadata(Session session, Identifier pid,
            SystemMetadata sysmeta) throws NotImplemented, NotAuthorized,
            ServiceFailure, InvalidRequest, InvalidSystemMetadata, InvalidToken {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean setObsoletedBy(Session session, Identifier pid,
            Identifier obsoletedByPid, long serialVersion)
            throws NotImplemented, NotFound, NotAuthorized, ServiceFailure,
            InvalidRequest, InvalidToken, VersionMismatch {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Identifier delete(Session session, Identifier id)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Identifier archive(Session session, Identifier id)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }



    @Override
    public void setNodeId(NodeReference nodeId) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setNodeType(NodeType nodeType) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public NodeType getNodeType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getLatestRequestUrl() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public InputStream get(Session session, Identifier id) throws InvalidToken,
            ServiceFailure, NotAuthorized, NotFound, NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SystemMetadata getSystemMetadata(Session session, Identifier id)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DescribeResponse describe(Session session, Identifier id)
            throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure,
            NotFound {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ObjectLocationList resolve(Session session, Identifier id)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Checksum getChecksum(Session session, Identifier pid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ObjectList listObjects(Session session, Date fromDate, Date toDate,
            ObjectFormatIdentifier formatId, NodeReference nodeId,
            Identifier identifier, Integer start, Integer count)
            throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
            ServiceFailure {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ObjectList search(Session session, String queryType, String query)
            throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
            NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public InputStream query(Session session, String queryEngine, String query)
            throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
            NotImplemented, NotFound {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryEngineDescription getQueryEngineDescription(Session session,
            String queryEngine) throws InvalidToken, ServiceFailure,
            NotAuthorized, NotImplemented, NotFound {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryEngineList listQueryEngines(Session session)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Identifier setRightsHolder(Session session, Identifier id,
            Subject userId, long serialVersion) throws InvalidToken,
            ServiceFailure, NotFound, NotAuthorized, NotImplemented,
            InvalidRequest, VersionMismatch {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isAuthorized(Session session, Identifier id,
            Permission permission) throws ServiceFailure, InvalidToken,
            NotFound, NotAuthorized, NotImplemented, InvalidRequest {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean setAccessPolicy(Session session, Identifier id,
            AccessPolicy policy, long serialVersion) throws InvalidToken,
            NotFound, NotImplemented, NotAuthorized, ServiceFailure,
            InvalidRequest, VersionMismatch {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Subject registerAccount(Session session, Person person)
            throws ServiceFailure, NotAuthorized, IdentifierNotUnique,
            InvalidCredentials, NotImplemented, InvalidRequest, InvalidToken {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Subject updateAccount(Session session, Person person)
            throws ServiceFailure, NotAuthorized, InvalidCredentials,
            NotImplemented, InvalidRequest, InvalidToken, NotFound {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean verifyAccount(Session session, Subject subject)
            throws ServiceFailure, NotAuthorized, NotImplemented, InvalidToken,
            InvalidRequest, NotFound {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public SubjectInfo getSubjectInfo(Session session, Subject subject)
            throws ServiceFailure, NotAuthorized, NotImplemented, NotFound,
            InvalidToken {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SubjectInfo listSubjects(Session session, String query,
            String status, Integer start, Integer count) throws InvalidRequest,
            ServiceFailure, InvalidToken, NotAuthorized, NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean mapIdentity(Session session, Subject primarySubject,
            Subject secondarySubject) throws ServiceFailure, InvalidToken,
            NotAuthorized, NotFound, NotImplemented, InvalidRequest,
            IdentifierNotUnique {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean requestMapIdentity(Session session, Subject subject)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
            NotImplemented, InvalidRequest, IdentifierNotUnique {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean confirmMapIdentity(Session session, Subject subject)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
            NotImplemented {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public SubjectInfo getPendingMapIdentity(Session session, Subject subject)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
            NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean denyMapIdentity(Session session, Subject subject)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
            NotImplemented {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean removeMapIdentity(Session session, Subject subject)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
            NotImplemented {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Subject createGroup(Session session, Group group)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotImplemented,
            IdentifierNotUnique, InvalidRequest {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean updateGroup(Session session, Group group)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
            NotImplemented, InvalidRequest {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean setReplicationStatus(Session session, Identifier pid,
            NodeReference nodeRef, ReplicationStatus status,
            BaseException failure) throws ServiceFailure, NotImplemented,
            InvalidToken, NotAuthorized, InvalidRequest, NotFound {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean setReplicationPolicy(Session session, Identifier pid,
            ReplicationPolicy policy, long serialVersion)
            throws NotImplemented, NotFound, NotAuthorized, ServiceFailure,
            InvalidRequest, InvalidToken, VersionMismatch {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isNodeAuthorized(Session session, Subject targetNodeSubject,
            Identifier pid) throws NotImplemented, NotAuthorized, InvalidToken,
            ServiceFailure, NotFound, InvalidRequest {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean updateReplicationMetadata(Session session, Identifier pid,
            Replica replicaMetadata, long serialVersion) throws NotImplemented,
            NotAuthorized, ServiceFailure, NotFound, InvalidRequest,
            InvalidToken, VersionMismatch {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean deleteReplicationMetadata(Session session, Identifier pid,
            NodeReference nodeId, long serialVersion) throws InvalidToken,
            InvalidRequest, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented, VersionMismatch {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public InputStream view(Session session, String theme, Identifier id)
            throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
            NotImplemented, NotFound {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public OptionList listViews() throws InvalidToken, ServiceFailure,
            NotAuthorized, InvalidRequest, NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SubjectInfo echoCredentials(Session session) throws NotImplemented,
            ServiceFailure, InvalidToken {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SystemMetadata echoSystemMetadata(Session session,
            SystemMetadata sysmeta) throws NotImplemented, ServiceFailure,
            NotAuthorized, InvalidToken, InvalidRequest, IdentifierNotUnique,
            InvalidSystemMetadata {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public InputStream echoIndexedObject(Session session, String queryEngine,
            SystemMetadata sysmeta, InputStream object) throws NotImplemented,
            ServiceFailure, NotAuthorized, InvalidToken, InvalidRequest,
            InvalidSystemMetadata, UnsupportedType, UnsupportedMetadataType,
            InsufficientResources {
        // TODO Auto-generated method stub
        return null;
    }
}
