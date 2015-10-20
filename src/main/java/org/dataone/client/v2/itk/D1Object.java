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

package org.dataone.client.v2.itk;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.util.ByteArrayDataSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.lang.StringUtils;
import org.dataone.client.types.AccessPolicyEditor;
import org.dataone.client.v2.CNode;
import org.dataone.client.v2.MNode;
import org.dataone.client.v2.formats.ObjectFormatCache;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.VersionMismatch;
import org.dataone.service.types.v1.AccessPolicy;
import org.dataone.service.types.v1.AccessRule;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v2.ObjectFormat;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ObjectLocation;
import org.dataone.service.types.v1.ObjectLocationList;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.Services;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.types.v1.util.ChecksumUtil;
import org.dataone.service.util.Constants;

/**
 * A representation of an object that can be stored in the DataONE system. Instances
 * have fields containing both their associated data and system metadata, and are differentiated
 * based on their ObjectFormat as stored in SystemMetadata.
 */
public class D1Object {

    private SystemMetadata sysmeta;
    
    private DataSource data;
    
    // Flag indicating whether the object already exists in a MN; set when the D1Object is created
    private boolean alreadyCreated = false;
    
    /**
     * Construct a new D1Object, which is then populated with data and system metadata
     * during the download process.
     */
    public D1Object() {
    	sysmeta = new SystemMetadata();
    }
    
    /**
     * Create an object that contains the system metadata and data bytes from the D1 system. The identifier
     * is first resolved against the Coordinating Node, and then both the data and system metadata for that
     * id are downloaded from the resolved Member Node and stored in the D1Object instance for easy retrieval.
     * @param id the Identifier to be retrieved from D1
     */
//    public D1Object(Identifier id) {
//        download(id);
//    }

    /**
     * Deprecated: in favor of the constructor that uses the ObjectFormatIdentifier, 
     * Subject, and NodeReference objects instead of string value for them.
     * 
     * Create an object that contains the given data bytes and with the given system metadata values. This 
     * constructor is used to build a D1Object locally in order to then call D1Object.create() to upload it 
     * to the proper Member Node.
     * 
     * @param id the identifier of the object
     * @param data the data bytes of the object
     * @param format the format of the object
     * @param submitter the submitter for the object
     * @param nodeId the identifier of the node on which the object will be created
     * @throws NoSuchAlgorithmException if the checksum algorithm does not exist
     * @throws IOException if the data bytes can not be read
     * @throws NotFound if the format specified is not found in the formatCache
     * @throws InvalidRequest if the content of parameters is not correct
     */
    @Deprecated
    public D1Object(Identifier id, byte[] data, String formatValue, String rightsHolderValue, String nodeIdValue) 
        throws NoSuchAlgorithmException, IOException, NotFound, InvalidRequest {
        alreadyCreated = false;
        this.data = new ByteArrayDataSource(data, formatValue);
        ObjectFormatIdentifier formatId = new ObjectFormatIdentifier();
        formatId.setValue(formatValue);
        Subject submitter = new Subject();
        submitter.setValue(rightsHolderValue);
        NodeReference nodeRef = new NodeReference();
        nodeRef.setValue(nodeIdValue);
        try {
			this.sysmeta = generateSystemMetadata(id, this.data.getInputStream(),
					formatId, submitter, nodeRef);
		} catch (ServiceFailure e) {
			// TODO: revisit whether these should be exposed (thrown)
			throw new NotFound("0","recast ServiceFailure: " + e.getDescription());
		} catch (NotImplemented e) {
			// TODO: revisit whether these should be exposed (thrown)
			throw new NotFound("0","recast NotImplemented: " + e.getDescription());
		}
    }
    
    
   /**
    * Deprecated in favor of constructor that takes a DataSource
    * 
    * Create an object that contains the given data bytes and with the given system metadata values. This 
    * constructor is used to build a D1Object locally in order to then call D1Object.create() to upload it 
    * to the proper Member Node.
    * 
    * @param id the identifier of the object
    * @param data the data bytes of the object
    * @param format the format of the object
    * @param rightsHolder the rightsHolder for the object
    * @param nodeId the identifier of the node on which the object will be created
    * @throws NoSuchAlgorithmException if the checksum algorithm does not exist
    * @throws IOException if the data bytes can not be read
    * @throws NotFound if the format specified is not found in the formatCache
    * @throws InvalidRequest if the content of parameters is not correct
    */
    @Deprecated
    public D1Object(Identifier id, byte[] data, ObjectFormatIdentifier formatId, Subject rightsHolder, NodeReference nodeId) throws NoSuchAlgorithmException,
            IOException, NotFound, InvalidRequest {
        alreadyCreated = false;
        this.data = new ByteArrayDataSource(data, (formatId == null ? null : formatId.getValue()));
        try {
            this.sysmeta = generateSystemMetadata(id, this.data.getInputStream(), formatId, rightsHolder, nodeId);
        } catch (ServiceFailure e) {
            // TODO: revisit whether these should be exposed (thrown)
            throw new NotFound("0", "recast ServiceFailure: " + e.getDescription());
        } catch (NotImplemented e) {
            // TODO: revisit whether these should be exposed (thrown)
            throw new NotFound("0", "recast NotImplemented: " + e.getDescription());
        }
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
     * @throws NoSuchAlgorithmException if the checksum algorithm does not exist
     * @throws IOException if the data bytes can not be read
     * @throws NotFound if the format specified is not found in the formatCache
     * @throws InvalidRequest if the content of parameters is not correct
     */
     public D1Object(Identifier id, DataSource data, ObjectFormatIdentifier formatId, Subject submitter, NodeReference nodeId) throws NoSuchAlgorithmException,
             IOException, NotFound, InvalidRequest {
         alreadyCreated = false;
         this.data = data;
         try {
             this.sysmeta = generateSystemMetadata(id, data.getInputStream(), formatId, submitter, nodeId);
         } catch (ServiceFailure e) {
             // TODO: revisit whether these should be exposed (thrown)
             throw new NotFound("0", "recast ServiceFailure: " + e.getDescription());
         } catch (NotImplemented e) {
             // TODO: revisit whether these should be exposed (thrown)
             throw new NotFound("0", "recast NotImplemented: " + e.getDescription());
         }
     }

    /**
     * @return the identifier
     */
    public Identifier getIdentifier() {
        return sysmeta.getIdentifier();
    }
    
    /**
     * Deprecated: use the method getFormatId() instead
     * @return the type
     */
    @Deprecated
    public ObjectFormatIdentifier getFmtId() {
        return sysmeta.getFormatId();
    }
    
    /**
     * @return the type
     */
    public ObjectFormatIdentifier getFormatId() {
        return sysmeta.getFormatId();
    }

    /**
     * @return the sysmeta
     */
    public SystemMetadata getSystemMetadata() {
        return sysmeta;
    }

    /**
     * @param sysmeta the sysmeta to set
     * @throws InvalidRequest - if the sysmeta parameter is null
     */
    public void setSystemMetadata(SystemMetadata sysmeta) throws InvalidRequest {
    	if (sysmeta == null) {
    		throw new InvalidRequest("Client Error", "sysmeta cannot be null");
    	}
        this.sysmeta = sysmeta;
    }

    /**
     * Deprecated in favor of the getDataSource method
     * 
     * @return the data
     * @throws IOException
     * @deprecated
     */
    public byte[] getData() throws IOException {
        return IOUtils.toByteArray(data.getInputStream());
    }

    /**
     * Deprecated in favor of the setDataSource method
     * 
     * @param data the data to set
     * @throws InvalidRequest - if data parameter is null
     * @deprecated
     */
    public void setData(byte[] data) throws InvalidRequest {
    	if (data == null) {
    		throw new InvalidRequest("Client Error", "data cannot be null");
    	}
        this.data = new ByteArrayDataSource(data, null);
    }
    
    /**
     * get the DataSource representing the data
     * @return
     * @since v1.2
     */
    public DataSource getDataSource()
    {
    	return data;
    }

    /**
     * set the DataSource representation of the data
     * @return
     * @since v1.2
     */
    public void setDataSource(DataSource dataSource) {
    	this.data = dataSource;
    }
    
    
    /**
     * Change the object to publicly readable
     * @param token the credentials to use to make the change
     * @throws ServiceFailure
     * @throws InvalidRequest 
     * @throws NotImplemented 
     * @throws NotAuthorized 
     * @throws NotFound 
     * @throws InvalidToken 
     * @throws VersionMismatch 
     */
    public void setPublicAccess(Session token) 
    throws ServiceFailure, InvalidToken, NotFound, NotAuthorized, NotImplemented, InvalidRequest, VersionMismatch 
    {
        AccessPolicy ap = new AccessPolicy();
        AccessRule ar = new AccessRule();
        Subject s = new Subject();
        s.setValue(Constants.SUBJECT_PUBLIC);
        ar.addSubject(s);
        ar.addPermission(Permission.READ);
        ap.addAllow(ar);
        
        if (alreadyCreated) {
        	// The object was already created on a MN, so we must set access policies on the CN
        	SystemMetadata smd = D1Client.getCN().getSystemMetadata(token, getIdentifier());
        	D1Client.getCN().setAccessPolicy(token, sysmeta.getIdentifier(), ap, smd.getSerialVersion().longValue());
        } else {
            // The object only exists locally, so we can set the access policy locally and it will be uploaded on create()
            sysmeta.setAccessPolicy(ap);
        }
    }
    
    /**
     * Contact D1 services to download the systemMetadata and data.  Multiple locations
     * will be tried until the data bytes are successfully downloaded.  Any exception
     * thrown will be from the latest location checked, or from the getSystemMetadata
     * call.
     * 
     * Users are advised to check the integrity of the download with the checkDataIntegrity()
     * method after the download.
     * 
     * @param id identifier to be downloaded
     * @throws InvalidToken 
     * @throws ServiceFailure 
     * @throws NotAuthorized 
     * @throws NotFound 
     * @throws NotImplemented 
     * @throws InsufficientResources 
     * @throws InvalidRequest - thrown when the data is retrieved but null
     */
    public static D1Object download(Identifier id) throws InvalidToken, ServiceFailure, 
    NotAuthorized, NotFound, NotImplemented, InsufficientResources, InvalidRequest 
    {
        
        D1Object o = null;
        boolean gotData = false;
        Exception latestException = null;
        try {	    
	        CNode cn = D1Client.getCN();

	        ObjectLocationList oll;
            // Get the system metadata for the object
            SystemMetadata m = cn.getSystemMetadata(null, id);
            if (m != null) {
                o = new D1Object();
                o.setSystemMetadata(m);
            }
            
            // Resolve the MNs that contain the object
            // Using resolve instead of the systemMetadata is the more formal way
            // to do this, because theoretically, resolve will aid in prioritizing
            // which node to go to first.
            oll = cn.resolve(null, id);
            
            
            // Try each of the locations until we find the object
            
            for (ObjectLocation ol : oll.getObjectLocationList()) {
                System.out.println("   === Trying Location: "
                        + ol.getNodeIdentifier().getValue() + " ("
                        + ol.getUrl() + ")");
                
                // Determine the service version for MNRead on the source MN
                org.dataone.client.v1.MNode v1mn = null;
                MNode v2mn = null;
                v1mn = org.dataone.client.v1.itk.D1Client.getMN(ol.getNodeIdentifier());
                List<Service> services = v1mn.getCapabilities().getServices().getServiceList();
                boolean v2GetIsAvailable = false;
                for (Service service : services ) {
                    String serviceName = service.getName();
                    String serviceVersion = service.getVersion();
                    Boolean isAvailable = service.getAvailable();
                    if ( serviceName == "MNRead" && 
                         serviceVersion.toLowerCase() == "v2" && 
                         isAvailable == true ) {
                        v2GetIsAvailable = true;
                        break;
                    }
                        
                }
                
                // Get the contents of the object itself
                InputStream inputStream = null;
                OutputStream outputStream = null;
                String tempDirStr = "";
                File tempFile = null;
                File tempDir = null;
                try {
                    if ( v2GetIsAvailable ) {
                        inputStream = v2mn.get(null, id);
                        
                    } else {
                        inputStream = v1mn.get(id);
                        
                    }
                    tempDirStr = Settings.getConfiguration().getString(
                            "D1Client.io.tmpdir", System.getProperty("java.io.tmpdir"));
                    if ( tempDirStr != null ) {
                        tempDir = new File(tempDirStr);
                    }
                    tempFile = File.createTempFile("d1_libclient_java.", ".tmp", tempDir);
                    DataSource dataSource = new FileDataSource(tempFile);
                    outputStream = dataSource.getOutputStream();
                    byte[] bytes = new byte[4096];
                    for (int len; (len = inputStream.read(bytes)) > 0; ) {
                        outputStream.write(bytes, 0, len);
                    }
                    o.setDataSource(dataSource);
                    //o.setData(IOUtils.toByteArray(inputStream));
                    gotData = true;
                    break;
 
                } catch (IOException e) {
                    latestException = e;
                    
                } catch (BaseException e) {
                    latestException = e;
                    
                } finally {
                    IOUtils.closeQuietly(inputStream);
                    IOUtils.closeQuietly(outputStream);
                    if ( tempFile != null ) {
                        if (tempFile.exists()) {
                            tempFile.deleteOnExit();
                        }                        
                    }                   
                }
            } 
        }
        catch (BaseException be) {
        	latestException = be;
        }
        
        if (!gotData) {
        	if (latestException != null) {
        		if (latestException instanceof InvalidToken) {
        			throw (InvalidToken) latestException;
        		}
        		else if (latestException instanceof ServiceFailure) {
        			throw (ServiceFailure) latestException;
        		}
        		else if (latestException instanceof NotAuthorized) {
        			throw (NotAuthorized) latestException;
        		}
        		else if (latestException instanceof NotFound) {
        			throw (NotFound) latestException;
        		}
        		else if (latestException instanceof NotImplemented) {
        			throw (NotImplemented) latestException;
        		}
        		else if (latestException instanceof InsufficientResources) {
        			throw (InsufficientResources) latestException;
        		}
        		else if (latestException instanceof InvalidRequest) {
        			throw (InvalidRequest) latestException;
        		}
        	} else {
        		throw new ServiceFailure("0000: Client Error", "Unexpected failure.  " +
        				"Could not get the request object, but no exception raised");		
        	}
        } 
        else {
            o.alreadyCreated = true;
        }

        return o;
    }

    
    /**
     * Generate a new system metadata object using the given input parameters. 
     * @param id the identifier of the object
     * @param data inputStream to the data
     * @param formatId the format identifier for the object.  If not found in the cache,
     *                   set the formatId to "application/octet-stream"
     * @param rightsHolder - the rightsHolder for the object if different from the submitter. can be null.
     * @param nodeId the identifier of the node on which the object will be created
     * @return the generated SystemMetadata instance
     * @throws NoSuchAlgorithmException if the checksum algorithm does not exist
     * @throws IOException if the data bytes can not be read
     * @throws NotFound if the objectFormat string cannot be found in the formatCache
     * @throws InvalidRequest if the parameter content is not correctly specified
     * @throws NotImplemented 
     * @throws ServiceFailure 
     */
    private SystemMetadata generateSystemMetadata(Identifier id, InputStream data, 
        	ObjectFormatIdentifier formatId, Subject rightsHolder, NodeReference nodeId) 
        throws NoSuchAlgorithmException, IOException, NotFound, InvalidRequest, ServiceFailure, NotImplemented 
        {
       	
        	validateRequest(id, "ignore".getBytes(), formatId, rightsHolder, nodeId);

        	SystemMetadata sm = new SystemMetadata();
        	sm.setIdentifier(id);
        	ObjectFormat fmt;
        	try {
        		fmt = ObjectFormatCache.getInstance().getFormat(formatId);
        	}
        	catch (BaseException be) {
        		formatId.setValue("application/octet-stream");
        		fmt = ObjectFormatCache.getInstance().getFormat(formatId);
        	}
        	sm.setFormatId(fmt.getFormatId());

        	//create the checksum
        	CountingInputStream cis = new CountingInputStream(data);

        	Checksum checksum;
        	checksum = ChecksumUtil.checksum(cis, "MD5");
        	sm.setChecksum(checksum);

        	//set the size
        	sm.setSize(new BigInteger(String.valueOf(cis.getByteCount())));

        	// the object serializer needs a value for this field, 
        	// though MNs will ignore the value
        	sm.setSerialVersion(BigInteger.ONE);
        	
        	// set rightsHolder from the associated string
        	if (rightsHolder != null)
        		sm.setRightsHolder(rightsHolder);
        	
        	Date dateCreated = new Date();
        	sm.setDateUploaded(dateCreated);
        	Date dateUpdated = new Date();
        	sm.setDateSysMetadataModified(dateUpdated);

        	// Node information
        	sm.setOriginMemberNode(nodeId);
        	sm.setAuthoritativeMemberNode(nodeId);

        	return sm;
        }
    
    
    
    
   
//    private SystemMetadata generateSystemMetadata(Identifier id, byte[] data, 
//    		ObjectFormatIdentifier formatId, Subject submitter, NodeReference nodeId) 
//            throws NoSuchAlgorithmException, IOException, NotFound, InvalidRequest, ServiceFailure, NotImplemented {
//
//    	    	
//    	validateRequest(id, data, formatId, submitter, nodeId);
//
//    	SystemMetadata sm = new SystemMetadata();
//    	sm.setIdentifier(id);
//    	ObjectFormat fmt;
//    	try {
//    		fmt = ObjectFormatCache.getInstance().getFormat(formatId);
//    	}
//    	catch (BaseException be) {
//    		formatId.setValue("application/octet-stream");
//    		fmt = ObjectFormatCache.getInstance().getFormat(formatId);
//    	}
//    	sm.setFormatId(fmt.getFormatId());
//
//    	//create the checksum
//    	InputStream is = new ByteArrayInputStream(data);
//
//    	Checksum checksum;
//    	checksum = ChecksumUtil.checksum(is, "MD5");
//    	sm.setChecksum(checksum);
//
//    	//set the size
//    	sm.setSize(new BigInteger(String.valueOf(data.length)));
//
//    	// serializer needs a value, though MN will ignore the value
//    	sm.setSerialVersion(BigInteger.ONE);
//    	
//    	// set submitter and rightholder from the associated string
//    	sm.setSubmitter(submitter);
//    	sm.setRightsHolder(submitter);
//    	
//    	// the dateUploaded field should be left blank - otherwise indicates
//    	// that the object is already created on a MN
////    	Date dateCreated = new Date();
////    	sm.setDateUploaded(dateCreated);
//    	
//    	// can be used to indicate the last time there was a change
//    	// but no guarantees
//    	Date dateChanged = new Date();
//    	sm.setDateSysMetadataModified(dateChanged);
//
//    	// Node information
//    	sm.setOriginMemberNode(nodeId);
//    	sm.setAuthoritativeMemberNode(nodeId);
//
//    	return sm;
//    }

    /**
     * 
     * Check the given set of input arguments that they are all valid and not null, 
     * and that string values are not null and of non-zero length. 
     * @param id
     * @param data
     * @param format
     * @param rightsHolder
     * @param nodeId
     * @throws InvalidRequest
     */
    protected static void validateRequest(Identifier id, byte[] data, ObjectFormatIdentifier formatId, Subject rightsHolder, 
            NodeReference nodeId) throws InvalidRequest {

        List<Object> objects = Arrays.asList((Object)id, (Object)data, (Object)formatId, (Object)rightsHolder, 
                (Object)nodeId);
        D1Object.checkNotNull(objects);
        // checks that the values of these objects are not null or empty ("");
        String invalidParams = "";
        if ( StringUtils.isEmpty( id.getValue() ) ) 
        	invalidParams += "'id' ";
        
        if ( StringUtils.isEmpty( formatId.getValue() ) ) 
        	invalidParams += "'formatId' ";
        
        if ( StringUtils.isEmpty( rightsHolder.getValue() ) ) 
        	invalidParams += "'rightsHolder' ";
        
        if ( StringUtils.isEmpty( nodeId.getValue() ) ) 
        	invalidParams += "'nodeId' ";
        
        if ( StringUtils.isNotEmpty(invalidParams) ) {
        	throw new InvalidRequest("0","values for " + invalidParams + "parameters were empty or null.");
        }
//        List<String> strings = Arrays.asList(id.getValue(), formatId.getValue(), submitter.getValue(), nodeId.getValue());
//        D1Object.checkLength(strings);        
    }
    
    /**
     * Check if any in a list of objects are null references.  If so, throw an exception.
     * @param objects the List of objects to check
     * @throws InvalidRequest if any Object in the list is null
     */
    public static void checkNotNull(List<Object> objects) throws InvalidRequest {
        for (Object obj : objects) {
            if (obj == null) {
                throw new InvalidRequest("0", "Parameter was null.  Provide all parameters.");
            }
        }
    }
    
    /**
     * Check that the all strings in the array have > 0 length.
     * @param strings
     * @throws InvalidRequest
     */
    protected static void checkLength(List<String> strings) throws InvalidRequest {
        for (String string : strings) {
            if (string.length() < 1) {
                throw new InvalidRequest("0", "String parameter had length 0.");
            }
        }
    }

    /**
     * Compares the data to the values in the size and checksum SystemMetadata fields
     * and returns true if they match.  Calculates the checksum from the data, so might
     * be processor intensive.
     * 
     * @return
     * @throws NoSuchAlgorithmException
     * @throws IOException 
     */
    public boolean checkDataIntegrity() throws NoSuchAlgorithmException, IOException
    {
    	Checksum calcd = ChecksumUtil.checksum(this.data.getInputStream(), 
    			this.sysmeta.getChecksum().getAlgorithm());
    	if (! calcd.getValue().equals( this.sysmeta.getChecksum().getValue() ) )
    		return false;
    	
    	return true;
    
    }
    
    /**
     * Provides an object to manipulate this D1Object's accessPolicy
     * @return
     * @since v1.1
     */
    public AccessPolicyEditor getAccessPolicyEditor() {
    	AccessPolicy ap = this.sysmeta.getAccessPolicy();
    	if (ap == null)
    		this.sysmeta.setAccessPolicy(new AccessPolicy());
    	
    	return new AccessPolicyEditor(this.sysmeta.getAccessPolicy());
    }
    
    
    /**
     * refresh the SystemMetadata of this object, first trying the CN, then the 
     * authoritative MN.  Will only do the refresh if what's found on the CN or MN
     * is more up-to-date than what's already here.  
     * 
     * @param retryTimeoutMS - the number of milliseconds to wait before giving up
     * on refresh.  If null, will only try the MN 1 time, otherwise will retry at 
     * regular intervals until the retryTimeout is reached.
     * @return boolean: true if a refresh took place, otherwise false
     * already had.
     * @throws ServiceFailure 
     * @throws NotImplemented 
     * @throws NotAuthorized 
     * @throws InvalidToken 
     * @throws InterruptedException
     * @since v1.2
     */
    public boolean refreshSystemMetadata(Integer retryTimeoutMS) 
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, InterruptedException 
    {
    	SystemMetadata smd = null;
    	if (retryTimeoutMS == null) 
    		retryTimeoutMS = 0;
    	try {
			smd = D1Client.getCN().getSystemMetadata(null, getIdentifier());
		} 
    	catch (BaseException be) {
			NodeReference nodeId = this.sysmeta.getAuthoritativeMemberNode();
			int interval = retryTimeoutMS > 40000 ? 10000 : 5000;
			for (int i=-1; i<retryTimeoutMS; i+=interval) {	
				try {
					smd = D1Client.getMN(nodeId).getSystemMetadata(null, getIdentifier());
					break;
				} catch (NotFound e) {
					if (i+interval < retryTimeoutMS) 
						Thread.sleep(interval);
					; // not exception within timeout window
				} 
			}
		}
    	if (smd == null) {
    		return false;
    	} else if (smd.getIdentifier().equals(this.getSystemMetadata().getIdentifier())) {
    		// don't update if the sysmeta we already is both from DataONE (has dataUploaded field)
    		// and the new serial version is greater what we already have
    		if (this.getSystemMetadata().getDateUploaded() != null &&
    			(smd.getSerialVersion().compareTo(this.getSystemMetadata().getSerialVersion()) > 0))
    		{ 
    			try {
    				this.setSystemMetadata(smd);
    			} catch (InvalidRequest e) {
    				; // with the explicit smd == null check, this should never be reached
    			}
    			return true;
    		} else {
    			// already up-to-date, so nothing has changed
    			return false;
    		}
    	} else {
    		throw new ServiceFailure("clientException","The identifier for the " +
    				"retrieved systemMetadata doesn't match that of the local version.");
    	}
    }
}
