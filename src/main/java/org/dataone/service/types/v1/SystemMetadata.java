
package org.dataone.service.types.v1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/** 
 * Schema fragment(s) for this class:
 * <pre>
 * &lt;xs:complexType xmlns:ns="http://ns.dataone.org/service/types/v1" xmlns:xs="http://www.w3.org/2001/XMLSchema" name="SystemMetadata">
 *   &lt;xs:sequence>
 *     &lt;xs:element type="ns:Identifier" name="identifier" minOccurs="1" maxOccurs="1"/>
 *     &lt;xs:element type="ns:ObjectFormat" name="objectFormat"/>
 *     &lt;xs:element type="xs:long" name="size"/>
 *     &lt;xs:element type="ns:Checksum" name="checksum" minOccurs="1" maxOccurs="1"/>
 *     &lt;xs:element type="ns:Subject" name="submitter"/>
 *     &lt;xs:element type="ns:Subject" name="rightsHolder"/>
 *     &lt;xs:element type="ns:AccessPolicy" name="accessPolicy" minOccurs="0" maxOccurs="1"/>
 *     &lt;xs:element type="ns:ReplicationPolicy" name="replicationPolicy" minOccurs="0" maxOccurs="1"/>
 *     &lt;xs:element type="ns:Identifier" name="obsoletes" minOccurs="0" maxOccurs="unbounded"/>
 *     &lt;xs:element type="ns:Identifier" name="obsoletedBy" minOccurs="0" maxOccurs="unbounded"/>
 *     &lt;xs:element type="ns:Identifier" name="resourceMap" minOccurs="0" maxOccurs="unbounded"/>
 *     &lt;xs:element type="xs:dateTime" name="dateUploaded"/>
 *     &lt;xs:element type="xs:dateTime" name="dateSysMetadataModified"/>
 *     &lt;xs:element type="ns:NodeReference" name="originMemberNode"/>
 *     &lt;xs:element type="ns:NodeReference" name="authoritativeMemberNode"/>
 *     &lt;xs:element type="ns:Replica" name="replica" minOccurs="0" maxOccurs="unbounded"/>
 *   &lt;/xs:sequence>
 * &lt;/xs:complexType>
 * </pre>
 */
public class SystemMetadata implements Serializable
{
    private Identifier identifier;
    private ObjectFormat objectFormat;
    private long size;
    private Checksum checksum;
    private Subject submitter;
    private Subject rightsHolder;
    private AccessPolicy accessPolicy;
    private ReplicationPolicy replicationPolicy;
    private List<Identifier> obsoleteList = new ArrayList<Identifier>();
    private List<Identifier> obsoletedByList = new ArrayList<Identifier>();
    private List<Identifier> resourceMapList = new ArrayList<Identifier>();
    private Date dateUploaded;
    private Date dateSysMetadataModified;
    private NodeReference originMemberNode;
    private NodeReference authoritativeMemberNode;
    private List<Replica> replicaList = new ArrayList<Replica>();

    /** 
     * Get the 'identifier' element value.
     * 
     * @return value
     */
    public Identifier getIdentifier() {
        return identifier;
    }

    /** 
     * Set the 'identifier' element value.
     * 
     * @param identifier
     */
    public void setIdentifier(Identifier identifier) {
        this.identifier = identifier;
    }

    /** 
     * Get the 'objectFormat' element value.
     * 
     * @return value
     */
    public ObjectFormat getObjectFormat() {
        return objectFormat;
    }

    /** 
     * Set the 'objectFormat' element value.
     * 
     * @param objectFormat
     */
    public void setObjectFormat(ObjectFormat objectFormat) {
        this.objectFormat = objectFormat;
    }

    /** 
     * Get the 'size' element value.
     * 
     * @return value
     */
    public long getSize() {
        return size;
    }

    /** 
     * Set the 'size' element value.
     * 
     * @param size
     */
    public void setSize(long size) {
        this.size = size;
    }

    /** 
     * Get the 'checksum' element value.
     * 
     * @return value
     */
    public Checksum getChecksum() {
        return checksum;
    }

    /** 
     * Set the 'checksum' element value.
     * 
     * @param checksum
     */
    public void setChecksum(Checksum checksum) {
        this.checksum = checksum;
    }

    /** 
     * Get the 'submitter' element value.
     * 
     * @return value
     */
    public Subject getSubmitter() {
        return submitter;
    }

    /** 
     * Set the 'submitter' element value.
     * 
     * @param submitter
     */
    public void setSubmitter(Subject submitter) {
        this.submitter = submitter;
    }

    /** 
     * Get the 'rightsHolder' element value.
     * 
     * @return value
     */
    public Subject getRightsHolder() {
        return rightsHolder;
    }

    /** 
     * Set the 'rightsHolder' element value.
     * 
     * @param rightsHolder
     */
    public void setRightsHolder(Subject rightsHolder) {
        this.rightsHolder = rightsHolder;
    }

    /** 
     * Get the 'accessPolicy' element value.
     * 
     * @return value
     */
    public AccessPolicy getAccessPolicy() {
        return accessPolicy;
    }

    /** 
     * Set the 'accessPolicy' element value.
     * 
     * @param accessPolicy
     */
    public void setAccessPolicy(AccessPolicy accessPolicy) {
        this.accessPolicy = accessPolicy;
    }

    /** 
     * Get the 'replicationPolicy' element value.
     * 
     * @return value
     */
    public ReplicationPolicy getReplicationPolicy() {
        return replicationPolicy;
    }

    /** 
     * Set the 'replicationPolicy' element value.
     * 
     * @param replicationPolicy
     */
    public void setReplicationPolicy(ReplicationPolicy replicationPolicy) {
        this.replicationPolicy = replicationPolicy;
    }

    /** 
     * Get the list of 'obsoletes' element items.
     * 
     * @return list
     */
    public List<Identifier> getObsoleteList() {
        return obsoleteList;
    }

    /** 
     * Set the list of 'obsoletes' element items.
     * 
     * @param list
     */
    public void setObsoleteList(List<Identifier> list) {
        obsoleteList = list;
    }

    /** 
     * Get the number of 'obsoletes' element items.
     * @return count
     */
    public int sizeObsoleteList() {
        return obsoleteList.size();
    }

    /** 
     * Add a 'obsoletes' element item.
     * @param item
     */
    public void addObsolete(Identifier item) {
        obsoleteList.add(item);
    }

    /** 
     * Get 'obsoletes' element item by position.
     * @return item
     * @param index
     */
    public Identifier getObsolete(int index) {
        return obsoleteList.get(index);
    }

    /** 
     * Remove all 'obsoletes' element items.
     */
    public void clearObsoleteList() {
        obsoleteList.clear();
    }

    /** 
     * Get the list of 'obsoletedBy' element items.
     * 
     * @return list
     */
    public List<Identifier> getObsoletedByList() {
        return obsoletedByList;
    }

    /** 
     * Set the list of 'obsoletedBy' element items.
     * 
     * @param list
     */
    public void setObsoletedByList(List<Identifier> list) {
        obsoletedByList = list;
    }

    /** 
     * Get the number of 'obsoletedBy' element items.
     * @return count
     */
    public int sizeObsoletedByList() {
        return obsoletedByList.size();
    }

    /** 
     * Add a 'obsoletedBy' element item.
     * @param item
     */
    public void addObsoletedBy(Identifier item) {
        obsoletedByList.add(item);
    }

    /** 
     * Get 'obsoletedBy' element item by position.
     * @return item
     * @param index
     */
    public Identifier getObsoletedBy(int index) {
        return obsoletedByList.get(index);
    }

    /** 
     * Remove all 'obsoletedBy' element items.
     */
    public void clearObsoletedByList() {
        obsoletedByList.clear();
    }

    /** 
     * Get the list of 'resourceMap' element items.
     * 
     * @return list
     */
    public List<Identifier> getResourceMapList() {
        return resourceMapList;
    }

    /** 
     * Set the list of 'resourceMap' element items.
     * 
     * @param list
     */
    public void setResourceMapList(List<Identifier> list) {
        resourceMapList = list;
    }

    /** 
     * Get the number of 'resourceMap' element items.
     * @return count
     */
    public int sizeResourceMapList() {
        return resourceMapList.size();
    }

    /** 
     * Add a 'resourceMap' element item.
     * @param item
     */
    public void addResourceMap(Identifier item) {
        resourceMapList.add(item);
    }

    /** 
     * Get 'resourceMap' element item by position.
     * @return item
     * @param index
     */
    public Identifier getResourceMap(int index) {
        return resourceMapList.get(index);
    }

    /** 
     * Remove all 'resourceMap' element items.
     */
    public void clearResourceMapList() {
        resourceMapList.clear();
    }

    /** 
     * Get the 'dateUploaded' element value.
     * 
     * @return value
     */
    public Date getDateUploaded() {
        return dateUploaded;
    }

    /** 
     * Set the 'dateUploaded' element value.
     * 
     * @param dateUploaded
     */
    public void setDateUploaded(Date dateUploaded) {
        this.dateUploaded = dateUploaded;
    }

    /** 
     * Get the 'dateSysMetadataModified' element value.
     * 
     * @return value
     */
    public Date getDateSysMetadataModified() {
        return dateSysMetadataModified;
    }

    /** 
     * Set the 'dateSysMetadataModified' element value.
     * 
     * @param dateSysMetadataModified
     */
    public void setDateSysMetadataModified(Date dateSysMetadataModified) {
        this.dateSysMetadataModified = dateSysMetadataModified;
    }

    /** 
     * Get the 'originMemberNode' element value.
     * 
     * @return value
     */
    public NodeReference getOriginMemberNode() {
        return originMemberNode;
    }

    /** 
     * Set the 'originMemberNode' element value.
     * 
     * @param originMemberNode
     */
    public void setOriginMemberNode(NodeReference originMemberNode) {
        this.originMemberNode = originMemberNode;
    }

    /** 
     * Get the 'authoritativeMemberNode' element value.
     * 
     * @return value
     */
    public NodeReference getAuthoritativeMemberNode() {
        return authoritativeMemberNode;
    }

    /** 
     * Set the 'authoritativeMemberNode' element value.
     * 
     * @param authoritativeMemberNode
     */
    public void setAuthoritativeMemberNode(NodeReference authoritativeMemberNode) {
        this.authoritativeMemberNode = authoritativeMemberNode;
    }

    /** 
     * Get the list of 'replica' element items.
     * 
     * @return list
     */
    public List<Replica> getReplicaList() {
        return replicaList;
    }

    /** 
     * Set the list of 'replica' element items.
     * 
     * @param list
     */
    public void setReplicaList(List<Replica> list) {
        replicaList = list;
    }

    /** 
     * Get the number of 'replica' element items.
     * @return count
     */
    public int sizeReplicaList() {
        return replicaList.size();
    }

    /** 
     * Add a 'replica' element item.
     * @param item
     */
    public void addReplica(Replica item) {
        replicaList.add(item);
    }

    /** 
     * Get 'replica' element item by position.
     * @return item
     * @param index
     */
    public Replica getReplica(int index) {
        return replicaList.get(index);
    }

    /** 
     * Remove all 'replica' element items.
     */
    public void clearReplicaList() {
        replicaList.clear();
    }
}
