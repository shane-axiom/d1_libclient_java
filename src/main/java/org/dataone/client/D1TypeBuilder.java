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
 * 
 * $Id$
 */

package org.dataone.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.Date;

import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.lang.StringUtils;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.AccessRule;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormat;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Person;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationPolicy;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.types.v1.util.AccessUtil;
import org.dataone.service.types.v1.util.ChecksumUtil;

public class D1TypeBuilder {


	public static NodeReference buildNodeReference(String value) {
		NodeReference id = new NodeReference();
		id.setValue(value);
		return id;
	}
	
	
	public static ObjectFormatIdentifier buildFormatIdentifier(String value) {
		ObjectFormatIdentifier fid = new ObjectFormatIdentifier();
		fid.setValue(value);
		return fid;
	}

	
	public static Identifier buildIdentifier(String value) {
		Identifier id = new Identifier();
		id.setValue(value);
		return id;
	}
	
	/**
	 * Validates the identifier checking for any invalid characters
	 * The only rule currently is no whitespace.
	 * @param identifier
	 * @return true if the identifier is valid
	 */
	//  The implementation below is not correct - it will only return false
	// the identifier is a single whitespace character

//	public static boolean isIdentifierValid(Identifier identifier)
//	{
//		String whiteSpaceRegex = "\\s";
//		if (identifier.getValue()..matches(whiteSpaceRegex)) {
//			return false;
//		}
//		
//		return true;
//	}
	
	
	public static Subject buildSubject(String value) 
	{
		Subject s = new Subject();
		s.setValue(value);
		return s;
	}


	public static AccessRule buildAccessRule(String subjectString, Permission permission)
	{
		if (subjectString == null || permission == null) {
			return null;
		}
		AccessRule ar = new AccessRule();
		ar.addSubject(buildSubject(subjectString));
		ar.addPermission(permission);
		return ar;
	}
	
	public static AccessRule buildAccessRule(String subjectString, Permission[] permissions)
	{
		if (subjectString == null || permissions == null) {
			return null;
		}
		AccessRule ar = new AccessRule();
		ar.addSubject(buildSubject(subjectString));

		for (Permission p: permissions)
			ar.addPermission(p);

		return ar;
	}


//	public static Person buildPerson(Subject subject, String familyName, 
//			String givenName, String emailString) 
//	{
//		String[] badParam = new String[]{};
//		Person person = new Person();
//		//	  try {
//			//		InternetAddress ia = new InternetAddress(emailString, true);
//		if (emailString == null || emailString.trim().equals(""))
//			badParam[badParam.length] = "emailString";
//		if (familyName == null || familyName.trim().equals(""))
//			badParam[badParam.length] = "familyName";
//		if (givenName == null || givenName.trim().equals(""))
//			badParam[badParam.length] = "givenName";
//		if (subject == null || subject.getValue().equals(""))
//			badParam[badParam.length] = "subject";
//
//		if (badParam.length > 0)
//			throw new IllegalArgumentException("null or empty string values for parameters: " + badParam);
//
//		//	} catch (AddressException e) {
//		//		// thrown by InternetAddress constructor
//		//	}
//
//		person.addEmail(emailString);
//		person.addGivenName(givenName);
//		person.setFamilyName(familyName);
//		person.setSubject(subject);
//		return person;
//	}
	
	/**
	 * Builds a minimal and 'typical' SystemMetadata object containing all of the required fields needed
	 * for submission to DataONE at time of create.  'Typical' in this case denotes
	 * that the rightsHolder and submitter are the same Subject. The formatId is nd the The Checksum and content length are 
	 * derived from the InputStream.  
	 * @param id
	 * @param data
	 * @param formatId
	 * @param rightsHolder
	 * @return
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 * @throws NotFound
	 * @throws ServiceFailure
	 */
	public static SystemMetadata buildMinimalSystemMetadata(Identifier id, InputStream data, 
			ObjectFormatIdentifier formatId, Subject rightsHolder) 
					throws NoSuchAlgorithmException, IOException, NotFound, ServiceFailure
	{
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

    	//generate the checksum and length fields from the inputStream
    	
    	CountingInputStream cis = new CountingInputStream(data);

    	Checksum checksum;
    	checksum = ChecksumUtil.checksum(cis, "MD5");
    	
    	sm.setChecksum(checksum);
    	sm.setSize(new BigInteger(String.valueOf( cis.getByteCount() )));
    	cis.close();
    	
    	// serializer needs a value, though MN will ignore the value
    	sm.setSerialVersion(BigInteger.ONE);
    	
    	// set submitter and rightholder from the associated string
    	sm.setSubmitter(rightsHolder);
    	sm.setRightsHolder(rightsHolder);
    	
    	Date dateCreated = new Date();
    	sm.setDateUploaded(dateCreated);
    	Date dateUpdated = new Date();
    	sm.setDateSysMetadataModified(dateUpdated);

    	// Node information
//    	sm.setOriginMemberNode(nodeId);
//    	sm.setAuthoritativeMemberNode(nodeId);

    	return sm;
	}
	
	/**
	 * Returns a clone of the given systemMetadata.  Note that some fields cloned,
	 * such as the replica list, are not necessarily functionally applicable to 
	 * the new  systemMetadata instance. for example, if you are using the clone 
	 * method to template the systemmetadta for a different object.
	 * 
	 * @param sm
	 * @return
	 */
	public static SystemMetadata cloneSystemMetadata(SystemMetadata sm) 
	{
		SystemMetadata clone = new SystemMetadata();
		
		clone.setAccessPolicy(AccessUtil.cloneAccessPolicy(sm.getAccessPolicy()));
		
		if (sm.getArchived() != null)
			clone.setArchived(new Boolean(sm.getArchived().booleanValue()));
		
		clone.setAuthoritativeMemberNode( D1TypeBuilder.cloneNodeReference(sm.getAuthoritativeMemberNode()) );

		clone.setChecksum( D1TypeBuilder.cloneChecksum(sm.getChecksum()) );

		clone.setDateSysMetadataModified((Date) sm.getDateSysMetadataModified().clone());
		clone.setDateUploaded((Date)sm.getDateUploaded().clone());

		clone.setFormatId(D1TypeBuilder.cloneFormatIdentifier(sm.getFormatId()));
		
		clone.setIdentifier(D1TypeBuilder.cloneIdentifier(sm.getIdentifier()));
		clone.setObsoletedBy(D1TypeBuilder.cloneIdentifier(sm.getObsoletedBy()));
		clone.setObsoletes(D1TypeBuilder.cloneIdentifier(sm.getObsoletes()));

		clone.setOriginMemberNode(D1TypeBuilder.cloneNodeReference(sm.getOriginMemberNode()));
		
		
		if (sm.getReplicaList() != null) {
			for (Replica rep: sm.getReplicaList()) {
				Replica newRep = new Replica();
				newRep.setReplicaMemberNode(D1TypeBuilder.cloneNodeReference(rep.getReplicaMemberNode()));
				newRep.setReplicationStatus(rep.getReplicationStatus());
				newRep.setReplicaVerified(rep.getReplicaVerified());
				clone.addReplica(newRep);
			}
		}

		
		if (sm.getReplicationPolicy() != null) {
			ReplicationPolicy rp = new ReplicationPolicy();
			rp.setNumberReplicas(sm.getReplicationPolicy().getNumberReplicas());
			rp.setReplicationAllowed(sm.getReplicationPolicy().getReplicationAllowed().booleanValue());
		
			if (sm.getReplicationPolicy().getBlockedMemberNodeList() != null) {
				for (NodeReference blockedMN : sm.getReplicationPolicy().getBlockedMemberNodeList()) {
					rp.addBlockedMemberNode(D1TypeBuilder.cloneNodeReference(blockedMN));
				}
			}
			
			if (sm.getReplicationPolicy().getPreferredMemberNodeList() != null) {
				for (NodeReference preferredMN : sm.getReplicationPolicy().getPreferredMemberNodeList()) {
					rp.addPreferredMemberNode(D1TypeBuilder.cloneNodeReference(preferredMN));
				}
			}
		}
			

		
		
		
		clone.setRightsHolder(D1TypeBuilder.cloneSubject(sm.getSubmitter()));
		clone.setSubmitter(D1TypeBuilder.cloneSubject(sm.getSubmitter()));
		
		if (sm.getSize() != null)
			clone.setSize(new BigInteger(String.valueOf(sm.getSize().longValue())));

		if (sm.getSerialVersion() != null)
			clone.setSerialVersion(new BigInteger(String.valueOf(sm.getSerialVersion().longValue())));
		
		
		return clone;
	}
	
	public static Identifier cloneIdentifier(Identifier orig) 
	{
		if (orig == null) return null;
		return D1TypeBuilder.buildIdentifier(orig.getValue());
	}
	
	public static Subject cloneSubject(Subject orig) 
	{
		if (orig == null) return null;
		return D1TypeBuilder.buildSubject(orig.getValue());
	}
	
	public static ObjectFormatIdentifier cloneFormatIdentifier(
			ObjectFormatIdentifier orig) 
	{
		if (orig == null) return null;
		return D1TypeBuilder.buildFormatIdentifier(orig.getValue());
	}
	
	
	public static NodeReference cloneNodeReference(NodeReference orig) 
	{
		if (orig == null) return null;
		return D1TypeBuilder.buildNodeReference(orig.getValue());
	}
	
	public static Checksum cloneChecksum(Checksum cs) 
	{
		if (cs == null) return null;
		
		Checksum clone = new Checksum();

		if(cs.getAlgorithm() != null)
			clone.setAlgorithm(cs.getAlgorithm());
		
		if (cs.getValue() != null)
			clone.setValue(cs.getValue());
	
		return clone;
	}
	
}
