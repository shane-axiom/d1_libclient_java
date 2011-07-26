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
import java.util.Arrays;
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
import org.dataone.service.types.v1.AccessPolicy;
import org.dataone.service.types.v1.AccessRule;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.ChecksumAlgorithm;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormat;
import org.dataone.service.types.v1.ObjectLocation;
import org.dataone.service.types.v1.ObjectLocationList;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.types.v1.util.ChecksumUtil;

/**
 * A representation of an object that can be stored in the DataONE system. Instances
 * have fields containing both their associated data and system metadata, and are differentiated
 * based on their ObjectFormat as stored in SystemMetadata.
 */
public class D1Object {

    private SystemMetadata sysmeta;
    private List<Identifier> describesList;
    private List<Identifier> describedByList;
    
    // TODO: this should also be able to be a reference to data, rather than a value, with late binding to allow efficient implementations
    private byte[] data;
    
    /**
     * Create an object that contains the system metadata and data bytes from the D1 system. The identifier
     * is first resolved against the Coordinating Node, and then both the data and system metadata for that
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
     * @throws NotFound 
     */
    public D1Object(Identifier id, byte[] data, String format, String submitter, String nodeId, 
            String[] describes, String[] describedBy) throws NoSuchAlgorithmException, IOException, NotFound {
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
        return this.describesList;
    }
    
    /**
     * @return the list of objects that describe this
     */
    public List<Identifier> getDescribeByList() {
        return this.describedByList;
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
     * @throws InvalidRequest 
     */
    public void create(Session token) throws InvalidToken, ServiceFailure, NotAuthorized, 
        IdentifierNotUnique, UnsupportedType, InsufficientResources, InvalidSystemMetadata, NotImplemented, InvalidRequest {
        
        // Check first that the identifier is not already in use
        CNode cn = D1Client.getCN();
        try {
            ObjectLocationList oll = cn.resolve(token, sysmeta.getIdentifier());
            // The object was found, so this ID is already used
            throw new IdentifierNotUnique("1120", "Identifier is already in use.  Please choose another and try create() again.");
        } catch (NotFound e) {
            // This is good -- we don't want to find the ID (or it would be in use already), 
            // so we purposely let this exception fall through to continue processing the create() call
        }
        
        // The ID is good, so insert into the MN
        String mn_url = D1Client.getCN().lookupNodeBaseUrl(sysmeta.getAuthoritativeMemberNode().getValue());
        MNode mn = D1Client.getMN(mn_url);
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        Identifier rGuid = mn.create(token, sysmeta.getIdentifier(), bis, sysmeta);
    }

    /**
     * Change the object to publicly readable on the MN
     * @param token the credentials to use to make the change
     * @throws ServiceFailure
     * @throws InvalidRequest 
     * @throws NotImplemented 
     * @throws NotAuthorized 
     * @throws NotFound 
     * @throws InvalidToken 
     */
    public void setPublicAccess(Session token) 
    throws ServiceFailure, InvalidToken, NotFound, NotAuthorized, NotImplemented, InvalidRequest 
    {
        String mn_url = D1Client.getCN().lookupNodeBaseUrl(sysmeta.getAuthoritativeMemberNode().getValue());
        MNode mn = D1Client.getMN(mn_url);
        AccessPolicy ap = new AccessPolicy();
        AccessRule ar = new AccessRule();
        Subject s = new Subject();
        s.setValue("public");
        ar.addSubject(s);
        ar.addPermission(Permission.READ);
        ap.addAllow(ar);
        mn.setAccessPolicy(token, sysmeta.getIdentifier(), ap);
    }
    
    /**
     * Contact D1 services to download the metadata and data.
     * @param id identifier to be downloaded
     */
    private void download(Identifier id) {
        try {

	    	D1Object o = null;
	    
	        CNode cn = D1Client.getCN();
	        Session token = new Session();
	    
	        ObjectLocationList oll;
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
     * @throws NotFound 
     */
    private SystemMetadata generateSystemMetadata(Identifier id, byte[] data, 
    		String format, String submitter, String nodeId, String[] describes, 
    		String[] describedBy) throws NoSuchAlgorithmException, IOException, NotFound {

    	//            validateRequest(id, data, format, submitter, nodeId, describes, describedBy);

    	SystemMetadata sm = new SystemMetadata();
    	sm.setIdentifier(id);
    	ObjectFormat fmt;
    	ObjectFormatCache ofc = ObjectFormatCache.getInstance();
    	try {
    		fmt = ofc.getFormat(format);
    		sm.setObjectFormat(fmt);
    	}
    	catch (NotFound nf) {
    		try {
    			fmt = ofc.getFormat("application/octet-stream");
    		} catch (NotFound nfe) {
    			throw nfe;
    		}
    	}

    	//create the checksum
    	InputStream is = new ByteArrayInputStream(data);
    	ChecksumAlgorithm ca = ChecksumAlgorithm.convert("MD5");
    	Checksum checksum;
    	checksum = ChecksumUtil.checksum(is, ca);
    	sm.setChecksum(checksum);

    	//set the size
    	sm.setSize(data.length);

    	//submitter
    	Subject p = new Subject();
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

//TODO reimplement when the OAI-ORE ResourceMap becomes available    	
/*
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
*/

    	return sm;
    }

    protected void validateRequest(Identifier id, byte[] data, String format, String submitter, 
            String nodeId, String[] describes, String[] describedBy) throws InvalidRequest {

        List<Object> objects = Arrays.asList((Object)id, (Object)data, (Object)format, (Object)submitter, 
                (Object)nodeId, (Object)describes, (Object)describedBy);
        D1Object.checkNotNull(objects);
        List<String> strings = Arrays.asList(id.getValue(), format.toString(), submitter, nodeId);
        D1Object.checkLength(strings);        
    }
    
    /**
     * Check if any in a list of objects are null references.  If so, throw an exception.
     * @param objects the List of objects to check
     * @throws InvalidRequest if any Object in the list is null
     */
    protected static void checkNotNull(List<Object> objects) throws InvalidRequest {
        for (Object obj : objects) {
            if (obj == null) {
                throw new InvalidRequest("0", "Parameter was null.  Provide all parameters.");
            }
        }
    }
    
    protected static void checkLength(List<String> strings) throws InvalidRequest {
        for (String string : strings) {
            if (string.length() < 1) {
                throw new InvalidRequest("0", "String paramter had length 0.");
            }
        }
    }
}
