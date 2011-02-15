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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;
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
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Checksum;
import org.dataone.service.types.ChecksumAlgorithm;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.NodeReference;
import org.dataone.service.types.ObjectFormat;
import org.dataone.service.types.ObjectLocation;
import org.dataone.service.types.ObjectLocationList;
import org.dataone.service.types.Principal;
import org.dataone.service.types.SystemMetadata;
import org.dataone.service.types.util.ServiceTypeUtil;

/**
 * A representation of an object that can be stored in the DataONE system. Instances
 * have fields containing both their associated data and system metadata, and are differentiated
 * based on their ObjectFormat as stored in SystemMetadata.
 */
public class D1Object {

    private SystemMetadata sysmeta;
    // TODO: this should also be able to be a reference to data, rather than a value, with late binding to allow efficient implementations
    private byte[] data;
    
    /**
     * Create an object that contains the system metadata and data bytes from the D1 system. The identifier
     * is first resolved against the Coordinating Node, and then boththe data and system metadata for that
     * id are downloaded from the resolved Member Node and stored in the D1Object instance for easy retrieval.
     * @param id the Identifier to be retrieved from D1
     */
    public D1Object(Identifier id) {
        download(id);
    }

    /**
     * Create an object that contains the given data bytes and with the given system metadata values. This 
     * constructor is used to build a D1Object locally in order to then call D1Object.create() to upload it 
     * to the proper Member Node.
     * 
     * @param id the identifier of the object
     * @param data the data bytes of the object
     * @param format the format of the object
     * @param submitter the submitter for the object
     * @param nodeId the identifier of the node on which the object will be created
     * @param describes the list of identifiers that this object describes
     * @param describedBy the list of objects described by this object
     * @throws NoSuchAlgorithmException if the checksum algorithm does not exist
     * @throws IOException if the data bytes can not be read
     */
    public D1Object(Identifier id, byte[] data, ObjectFormat format, String submitter, String nodeId, 
            String[] describes, String[] describedBy) throws NoSuchAlgorithmException, IOException {
        this.data = data;
        this.sysmeta = generateSystemMetadata(id, data, format, submitter, nodeId, describes, describedBy);
    }

    /**
     * @return the identifier
     */
    public Identifier getIdentifier() {
        return sysmeta.getIdentifier();
    }
    
    /**
     * @return the type
     */
    public ObjectFormat getType() {
        return sysmeta.getObjectFormat();
    }

    /**
     * @return the sysmeta
     */
    public SystemMetadata getSystemMetadata() {
        return sysmeta;
    }

    /**
     * @param sysmeta the sysmeta to set
     */
    public void setSystemMetadata(SystemMetadata sysmeta) {
        assert(sysmeta != null);
        this.sysmeta = sysmeta;
    }

    /**
     * @return the data
     */
    public byte[] getData() {
        return data;
    }

    /**
     * @param data the data to set
     */
    public void setData(byte[] data) {
        assert(data != null);
        this.data = data;
    }

    /**
     * @return the list of objects this describes
     */
    public List<Identifier> getDescribeList() {
        return sysmeta.getDescribeList();
    }
    
    /**
     * @return the list of objects that describe this
     */
    public List<Identifier> getDescribeByList() {
        return sysmeta.getDescribedByList();
    }

    /**
     * @return the list of objects that obsolete this
     */
    public List<Identifier> getObsoletedByList() {
        return sysmeta.getObsoletedByList();
    }
    /**
     * Create the object on the associated member node that is present in the system metadata
     * for the D1Object. Assumes that the object has not already been created.  If it
     * already exists, an exception will be thrown.
     * 
     * @param token the session token to be used to create the object
     * @throws InvalidToken
     * @throws ServiceFailure
     * @throws NotAuthorized
     * @throws IdentifierNotUnique
     * @throws UnsupportedType
     * @throws InsufficientResources
     * @throws InvalidSystemMetadata
     * @throws NotImplemented
     */
    public void create(AuthToken token) throws InvalidToken, ServiceFailure, NotAuthorized, 
        IdentifierNotUnique, UnsupportedType, InsufficientResources, InvalidSystemMetadata, NotImplemented {
        
        MNode mn = D1Client.getMN(sysmeta.getAuthoritativeMemberNode().getValue());
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        Identifier rGuid = mn.create(token, sysmeta.getIdentifier(), bis, sysmeta);
    }

    /**
     * Change the object to publicly readable on the MN
     * @param token the credentials to use to make the change
     * @throws ServiceFailure
     */
    public void setPublicAccess(AuthToken token) throws ServiceFailure {
        MNode mn = D1Client.getMN(sysmeta.getAuthoritativeMemberNode().getValue());
        mn.setAccess(token, sysmeta.getIdentifier(), "public", "read", "allow", "allowFirst");
    }
    
    /**
     * Contact D1 services to download the metadata and data.
     * @param id identifier to be downloaded
     */
    private void download(Identifier id) {
        D1Object o = null;
    
        CNode cn = D1Client.getCN();
        AuthToken token = new AuthToken();
    
        ObjectLocationList oll;
        try {
            // Get the system metadata for the object
            SystemMetadata m = cn.getSystemMetadata(token, id);
            if (m != null) {
                setSystemMetadata(m);
            }
            
            // Resolve the MNs that contain the object
            oll = cn.resolve(token, id);
            boolean gotData = false;
            // Try each of the locations until we find the object
            for (ObjectLocation ol : oll.getObjectLocationList()) {
                System.out.println("   === Trying Location: "
                        + ol.getNodeIdentifier().getValue() + " ("
                        + ol.getUrl() + ")");
                               
                // Get the contents of the object itself
                MNode mn = D1Client.getMN(ol.getBaseURL());
                try {
                    InputStream is = mn.get(token, id);
                    try {
                        setData(IOUtils.toByteArray(is));
                        gotData = true;
                        break;
                    } catch (IOException e) {
                        // Couldn't get the object from this object location
                        // So move on to the next
                        e.printStackTrace();
                    }
                } catch (InvalidToken e) {
                    e.printStackTrace();
                } catch (ServiceFailure e) {
                    e.printStackTrace();
                } catch (NotAuthorized e) {
                    e.printStackTrace();
                } catch (NotFound e) {
                    e.printStackTrace();
                } catch (NotImplemented e) {
                    e.printStackTrace();
                }
            }
            if (!gotData) {
                System.out.println("Never found the data on MN.");
            }
        } catch (InvalidToken e) {
            e.printStackTrace();
        } catch (ServiceFailure e) {
            e.printStackTrace();
        } catch (NotAuthorized e) {
            e.printStackTrace();
        } catch (NotFound e) {
            e.printStackTrace();
        } catch (InvalidRequest e) {
            e.printStackTrace();
        } catch (NotImplemented e) {
            e.printStackTrace();
        }
    }

    /**
     * Generate a new system metadata object using the given input parameters. 
     * @param id the identifier of the object
     * @param data the data bytes of the object
     * @param format the format of the object
     * @param submitter the submitter for the object
     * @param nodeId the identifier of the node on which the object will be created
     * @param describes the list of identifiers that this object describes
     * @param describedBy the list of objects described by this object
     * @return the generated SystemMetadata instance
     * @throws NoSuchAlgorithmException if the checksum algorithm does not exist
     * @throws IOException if the data bytes can not be read
     */
    private SystemMetadata generateSystemMetadata(Identifier id, byte[] data, ObjectFormat format, String submitter, String nodeId, 
                String[] describes, String[] describedBy) throws NoSuchAlgorithmException, IOException {
        
            SystemMetadata sm = new SystemMetadata();
            sm.setIdentifier(id);
            sm.setObjectFormat(format);
            
            //create the checksum
            InputStream is = new ByteArrayInputStream(data);
            ChecksumAlgorithm ca = ChecksumAlgorithm.convert("MD5");
            Checksum checksum;
            checksum = ServiceTypeUtil.checksum(is, ca);
            sm.setChecksum(checksum);
    
            //set the size
            sm.setSize(data.length);
    
            //submitter
            Principal p = new Principal();
            p.setValue(submitter);
            sm.setSubmitter(p);
            sm.setRightsHolder(p);
            Date dateCreated = new Date();
            sm.setDateUploaded(dateCreated);
            Date dateUpdated = new Date();
            sm.setDateSysMetadataModified(dateUpdated);
    
            // Node information
            NodeReference nr = new NodeReference();
            nr.setValue(nodeId);
            sm.setOriginMemberNode(nr);
            sm.setAuthoritativeMemberNode(nr);
            
            // Describes and describedBy
            for (String describesId : describes) {
                Identifier dId = new Identifier();
                dId.setValue(describesId);
                sm.addDescribe(dId);
            }
            for (String describedById : describedBy) {
                Identifier dId = new Identifier();
                dId.setValue(describedById);
                sm.addDescribedBy(dId);
            }
            
            return sm;
        }
}
