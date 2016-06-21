package org.dataone.client.v1.itk;

import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.dataone.client.v1.CNode;
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
import org.dataone.service.types.v1.NodeType;
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
import org.dataone.service.types.v1.TypeFactory;
import org.dataone.service.types.v1_1.QueryEngineDescription;
import org.dataone.service.types.v1_1.QueryEngineList;

public class CNRegisterSkeleton implements CNode {

    private static NodeList nodeList = new NodeList();
    private static int nextIndex = 0;
    private static Map<NodeReference,Integer> nodeIndexMap = new HashMap<>();
    private NodeReference nodeId;
    private String baseUrl;

    public CNRegisterSkeleton() throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, InvalidToken, IdentifierNotUnique {
        this(TypeFactory.buildNodeReference("urn:node:CnRegisterSkeleton"), "java:" + CNRegisterSkeleton.class.getCanonicalName());
    }
    
    public CNRegisterSkeleton(NodeReference nodeId, String baseUrl) throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest, InvalidToken, IdentifierNotUnique {
        this.nodeId = nodeId;
        this.baseUrl = baseUrl;
        Node cnSelf = new Node();
        cnSelf.setBaseURL(baseUrl);
        cnSelf.setIdentifier(nodeId);
        cnSelf.setType(NodeType.CN);
        register(null,cnSelf);
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

    //    @Override
    //    public Node getNodeCapabilities(NodeReference nodeid)
    //            throws NotImplemented, ServiceFailure, InvalidRequest, NotFound {
    //        // TODO Auto-generated method stub
    //        if (nodeIndexMap.containsKey(nodeid)) {
    //            return nodeList.getNode(nodeIndexMap.get(nodeid));
    //        } else {
    //            throw new NotFound("00,", "Node not found");
    //        }
    //    }

    @Override
    public NodeList listNodes() throws NotImplemented, ServiceFailure {
        // TODO Auto-generated method stub
        return CNRegisterSkeleton.nodeList;
    }

    @Override
    public String getNodeBaseServiceUrl() {
        return this.baseUrl;
    }
    
    public void setNodeBaseServiceUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    @Override
    public NodeReference getNodeId() {
        return nodeId;
    }

    @Override
    public NodeType getNodeType() {
        return NodeType.CN;
    }
    
    
    /////////////////////////////////////////////////
    
    @Override
    public void setNodeId(NodeReference nodeId) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setNodeType(NodeType nodeType) {
        // TODO Auto-generated method stub

    }

    @Override
    public String getLatestRequestUrl() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Date ping() throws NotImplemented, ServiceFailure, InsufficientResources {
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
    public ChecksumAlgorithmList listChecksumAlgorithms() throws ServiceFailure,
    NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Log getLogRecords(Date fromDate, Date toDate, Event event,
            String pidFilter, Integer start, Integer count) throws InvalidToken,
            InvalidRequest, ServiceFailure, NotAuthorized, NotImplemented,
            InsufficientResources {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Identifier reserveIdentifier(Identifier pid) throws InvalidToken,
    ServiceFailure, NotAuthorized, IdentifierNotUnique, NotImplemented,
    InvalidRequest {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Identifier generateIdentifier(String scheme, String fragment)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented,
            InvalidRequest {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean hasReservation(Subject subject, Identifier pid)
            throws InvalidToken, ServiceFailure, NotFound, NotAuthorized,
            NotImplemented, InvalidRequest {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Identifier create(Identifier pid, InputStream object,
            SystemMetadata sysmeta) throws InvalidToken, ServiceFailure,
            NotAuthorized, IdentifierNotUnique, UnsupportedType,
            InsufficientResources, InvalidSystemMetadata, NotImplemented,
            InvalidRequest {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Identifier registerSystemMetadata(Identifier pid, SystemMetadata sysmeta)
            throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest,
            InvalidSystemMetadata, InvalidToken {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean setObsoletedBy(Identifier pid, Identifier obsoletedByPid,
            long serialVersion) throws NotImplemented, NotFound, NotAuthorized,
            ServiceFailure, InvalidRequest, InvalidToken, VersionMismatch {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Identifier delete(Identifier pid) throws InvalidToken, ServiceFailure,
    NotAuthorized, NotFound, NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Identifier archive(Identifier pid) throws InvalidToken, ServiceFailure,
    NotAuthorized, NotFound, NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public Log getLogRecords(Session session, Date fromDate, Date toDate,
            Event event, String pidFilter, Integer start, Integer count)
                    throws InvalidToken, InvalidRequest, ServiceFailure, NotAuthorized,
                    NotImplemented, InsufficientResources {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public Identifier reserveIdentifier(Session session, Identifier pid)
            throws InvalidToken, ServiceFailure, NotAuthorized,
            IdentifierNotUnique, NotImplemented, InvalidRequest {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public Identifier generateIdentifier(Session session, String scheme,
            String fragment) throws InvalidToken, ServiceFailure, NotAuthorized,
            NotImplemented, InvalidRequest {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public boolean hasReservation(Session session, Subject subject, Identifier pid)
            throws InvalidToken, ServiceFailure, NotFound, NotAuthorized,
            NotImplemented, InvalidRequest {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Deprecated
    public Identifier create(Session session, Identifier pid, InputStream object,
            SystemMetadata sysmeta) throws InvalidToken, ServiceFailure,
            NotAuthorized, IdentifierNotUnique, UnsupportedType,
            InsufficientResources, InvalidSystemMetadata, NotImplemented,
            InvalidRequest {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public Identifier registerSystemMetadata(Session session, Identifier pid,
            SystemMetadata sysmeta) throws NotImplemented, NotAuthorized,
            ServiceFailure, InvalidRequest, InvalidSystemMetadata, InvalidToken {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public boolean setObsoletedBy(Session session, Identifier pid,
            Identifier obsoletedByPid, long serialVersion) throws NotImplemented,
            NotFound, NotAuthorized, ServiceFailure, InvalidRequest, InvalidToken,
            VersionMismatch {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Deprecated
    public Identifier delete(Session session, Identifier pid) throws InvalidToken,
    ServiceFailure, InvalidRequest, NotAuthorized, NotFound, NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public Identifier archive(Session session, Identifier pid) throws InvalidToken,
    ServiceFailure, InvalidRequest, NotAuthorized, NotFound, NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public InputStream get(Identifier pid) throws InvalidToken, ServiceFailure,
    NotAuthorized, NotFound, NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SystemMetadata getSystemMetadata(Identifier pid) throws InvalidToken,
    ServiceFailure, NotAuthorized, NotFound, NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DescribeResponse describe(Identifier pid) throws InvalidToken,
    NotAuthorized, NotImplemented, ServiceFailure, NotFound {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ObjectLocationList resolve(Identifier pid) throws InvalidToken,
    ServiceFailure, NotAuthorized, NotFound, NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Checksum getChecksum(Identifier pid) throws InvalidToken,
    ServiceFailure, NotAuthorized, NotFound, NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ObjectList listObjects(Date fromDate, Date toDate,
            ObjectFormatIdentifier formatId, Boolean replicaStatus, Integer start,
            Integer count) throws InvalidRequest, InvalidToken, NotAuthorized,
            NotImplemented, ServiceFailure {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ObjectList search(String queryType, String query) throws InvalidToken,
    ServiceFailure, NotAuthorized, InvalidRequest, NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public InputStream query(String queryEngine, String query) throws InvalidToken,
    ServiceFailure, NotAuthorized, InvalidRequest, NotImplemented, NotFound {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryEngineDescription getQueryEngineDescription(String queryEngine)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented,
            NotFound {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryEngineList listQueryEngines() throws InvalidToken, ServiceFailure,
    NotAuthorized, NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public InputStream get(Session session, Identifier pid) throws InvalidToken,
    ServiceFailure, NotAuthorized, NotFound, NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public SystemMetadata getSystemMetadata(Session session, Identifier pid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public DescribeResponse describe(Session session, Identifier pid)
            throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure,
            NotFound {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public ObjectLocationList resolve(Session session, Identifier pid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public Checksum getChecksum(Session session, Identifier pid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public ObjectList listObjects(Session session, Date fromDate, Date toDate,
            ObjectFormatIdentifier formatId, Boolean replicaStatus, Integer start,
            Integer count) throws InvalidRequest, InvalidToken, NotAuthorized,
            NotImplemented, ServiceFailure {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public ObjectList search(Session session, String queryType, String query)
            throws InvalidToken, ServiceFailure, NotAuthorized, InvalidRequest,
            NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Identifier setRightsHolder(Identifier pid, Subject userId,
            long serialVersion) throws InvalidToken, ServiceFailure, NotFound,
            NotAuthorized, NotImplemented, InvalidRequest, VersionMismatch {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isAuthorized(Identifier pid, Permission permission)
            throws ServiceFailure, InvalidToken, NotFound, NotAuthorized,
            NotImplemented, InvalidRequest {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean setAccessPolicy(Identifier pid, AccessPolicy policy,
            long serialVersion) throws InvalidToken, NotFound, NotImplemented,
            NotAuthorized, ServiceFailure, InvalidRequest, VersionMismatch {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Deprecated
    public Identifier setRightsHolder(Session session, Identifier pid,
            Subject userId, long serialVersion) throws InvalidToken,
            ServiceFailure, NotFound, NotAuthorized, NotImplemented,
            InvalidRequest, VersionMismatch {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public boolean isAuthorized(Session session, Identifier pid,
            Permission permission) throws ServiceFailure, InvalidToken, NotFound,
            NotAuthorized, NotImplemented, InvalidRequest {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Deprecated
    public boolean setAccessPolicy(Session session, Identifier pid,
            AccessPolicy policy, long serialVersion) throws InvalidToken, NotFound,
            NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest,
            VersionMismatch {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Subject registerAccount(Person person) throws ServiceFailure,
    NotAuthorized, IdentifierNotUnique, InvalidCredentials, NotImplemented,
    InvalidRequest, InvalidToken {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Subject updateAccount(Person person) throws ServiceFailure,
    NotAuthorized, InvalidCredentials, NotImplemented, InvalidRequest,
    InvalidToken, NotFound {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean verifyAccount(Subject subject) throws ServiceFailure,
    NotAuthorized, NotImplemented, InvalidToken, InvalidRequest, NotFound {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public SubjectInfo getSubjectInfo(Subject subject) throws ServiceFailure,
    NotAuthorized, NotImplemented, NotFound, InvalidToken {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SubjectInfo listSubjects(String query, String status, Integer start,
            Integer count) throws InvalidRequest, ServiceFailure, InvalidToken,
            NotAuthorized, NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean mapIdentity(Subject primarySubject, Subject secondarySubject)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
            NotImplemented, InvalidRequest, IdentifierNotUnique {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean requestMapIdentity(Subject subject) throws ServiceFailure,
    InvalidToken, NotAuthorized, NotFound, NotImplemented, InvalidRequest,
    IdentifierNotUnique {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean confirmMapIdentity(Subject subject) throws ServiceFailure,
    InvalidToken, NotAuthorized, NotFound, NotImplemented {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public SubjectInfo getPendingMapIdentity(Subject subject)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
            NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean denyMapIdentity(Subject subject) throws ServiceFailure,
    InvalidToken, NotAuthorized, NotFound, NotImplemented {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean removeMapIdentity(Subject subject) throws ServiceFailure,
    InvalidToken, NotAuthorized, NotFound, NotImplemented {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Subject createGroup(Group group) throws ServiceFailure, InvalidToken,
    NotAuthorized, NotImplemented, IdentifierNotUnique, InvalidRequest {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean updateGroup(Group group) throws ServiceFailure, InvalidToken,
    NotAuthorized, NotFound, NotImplemented, InvalidRequest {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Deprecated
    public Subject registerAccount(Session session, Person person)
            throws ServiceFailure, NotAuthorized, IdentifierNotUnique,
            InvalidCredentials, NotImplemented, InvalidRequest, InvalidToken {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public Subject updateAccount(Session session, Person person)
            throws ServiceFailure, NotAuthorized, InvalidCredentials,
            NotImplemented, InvalidRequest, InvalidToken, NotFound {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public boolean verifyAccount(Session session, Subject subject)
            throws ServiceFailure, NotAuthorized, NotImplemented, InvalidToken,
            InvalidRequest, NotFound {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Deprecated
    public SubjectInfo getSubjectInfo(Session session, Subject subject)
            throws ServiceFailure, NotAuthorized, NotImplemented, NotFound,
            InvalidToken {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public SubjectInfo listSubjects(Session session, String query, String status,
            Integer start, Integer count) throws InvalidRequest, ServiceFailure,
            InvalidToken, NotAuthorized, NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public boolean mapIdentity(Session session, Subject primarySubject,
            Subject secondarySubject) throws ServiceFailure, InvalidToken,
            NotAuthorized, NotFound, NotImplemented, InvalidRequest,
            IdentifierNotUnique {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Deprecated
    public boolean requestMapIdentity(Session session, Subject subject)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
            NotImplemented, InvalidRequest, IdentifierNotUnique {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Deprecated
    public boolean confirmMapIdentity(Session session, Subject subject)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
            NotImplemented {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Deprecated
    public SubjectInfo getPendingMapIdentity(Session session, Subject subject)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
            NotImplemented {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public boolean denyMapIdentity(Session session, Subject subject)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
            NotImplemented {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Deprecated
    public boolean removeMapIdentity(Session session, Subject subject)
            throws ServiceFailure, InvalidToken, NotAuthorized, NotFound,
            NotImplemented {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Deprecated
    public Subject createGroup(Session session, Group group) throws ServiceFailure,
    InvalidToken, NotAuthorized, NotImplemented, IdentifierNotUnique,
    InvalidRequest {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public boolean updateGroup(Session session, Group group) throws ServiceFailure,
    InvalidToken, NotAuthorized, NotFound, NotImplemented, InvalidRequest {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean updateNodeCapabilities(NodeReference nodeid, Node node)
            throws NotImplemented, NotAuthorized, ServiceFailure, InvalidRequest,
            NotFound, InvalidToken {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public NodeReference register(Node node) throws NotImplemented, NotAuthorized,
    ServiceFailure, InvalidRequest, InvalidToken, IdentifierNotUnique {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Deprecated
    public boolean updateNodeCapabilities(Session session, NodeReference nodeid,
            Node node) throws NotImplemented, NotAuthorized, ServiceFailure,
            InvalidRequest, NotFound, InvalidToken {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean setReplicationStatus(Identifier pid, NodeReference nodeRef,
            ReplicationStatus status, BaseException failure) throws ServiceFailure,
            NotImplemented, InvalidToken, NotAuthorized, InvalidRequest, NotFound {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean setReplicationPolicy(Identifier pid, ReplicationPolicy policy,
            long serialVersion) throws NotImplemented, NotFound, NotAuthorized,
            ServiceFailure, InvalidRequest, InvalidToken, VersionMismatch {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isNodeAuthorized(Subject targetNodeSubject, Identifier pid)
            throws NotImplemented, NotAuthorized, InvalidToken, ServiceFailure,
            NotFound, InvalidRequest {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean updateReplicationMetadata(Identifier pid,
            Replica replicaMetadata, long serialVersion) throws NotImplemented,
            NotAuthorized, ServiceFailure, NotFound, InvalidRequest, InvalidToken,
            VersionMismatch {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean deleteReplicationMetadata(Identifier pid, NodeReference nodeId,
            long serialVersion) throws InvalidToken, InvalidRequest,
            ServiceFailure, NotAuthorized, NotFound, NotImplemented,
            VersionMismatch {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Deprecated
    public boolean setReplicationStatus(Session session, Identifier pid,
            NodeReference nodeRef, ReplicationStatus status, BaseException failure)
                    throws ServiceFailure, NotImplemented, InvalidToken, NotAuthorized,
                    InvalidRequest, NotFound {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Deprecated
    public boolean setReplicationPolicy(Session session, Identifier pid,
            ReplicationPolicy policy, long serialVersion) throws NotImplemented,
            NotFound, NotAuthorized, ServiceFailure, InvalidRequest, InvalidToken,
            VersionMismatch {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Deprecated
    public boolean isNodeAuthorized(Session originatingNodeSession,
            Subject targetNodeSubject, Identifier pid) throws NotImplemented,
            NotAuthorized, InvalidToken, ServiceFailure, NotFound, InvalidRequest {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Deprecated
    public boolean updateReplicationMetadata(Session targetNodeSession,
            Identifier pid, Replica replicaMetadata, long serialVersion)
                    throws NotImplemented, NotAuthorized, ServiceFailure, NotFound,
                    InvalidRequest, InvalidToken, VersionMismatch {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    @Deprecated
    public boolean deleteReplicationMetadata(Session session, Identifier pid,
            NodeReference nodeId, long serialVersion) throws InvalidToken,
            InvalidRequest, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented, VersionMismatch {
        // TODO Auto-generated method stub
        return false;
    }

    ////////////  the rest of the methods are no-ops  //////////////    


}
