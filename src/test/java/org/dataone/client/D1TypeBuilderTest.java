package org.dataone.client;

import static org.junit.Assert.*;

import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.Date;

import org.dataone.client.v1.types.D1TypeBuilder;
import org.dataone.service.types.v1.AccessRule;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.types.v1.util.AccessUtil;
import org.dataone.service.types.v1.util.ChecksumUtil;
import org.junit.Test;

public class D1TypeBuilderTest {

	@Test
	public void testBuildNodeReference() {
		String val = "foo";
		NodeReference nr = D1TypeBuilder.buildNodeReference(val);
		assertTrue("a new object containing the correct value is built",
				nr.getValue().equals(val));
	}

	@Test
	public void testBuildFormatIdentifier() {
		String val = "foo";
		ObjectFormatIdentifier formatId = D1TypeBuilder.buildFormatIdentifier(val);
		assertTrue("a new object containing the correct value is built",
				formatId.getValue().equals(val));
	}

	@Test
	public void testBuildIdentifier() {
		String val = "foo";
		Identifier id = D1TypeBuilder.buildIdentifier(val);
		assertTrue("a new object containing the correct value is built",
				id.getValue().equals(val));
	}

//	@Test
//	public void testIsIdentifierValid() {
//		Identifier pid = D1TypeBuilder.buildIdentifier("ho ho ho");
//		assertFalse("identifier containing spaces should return false ['" + pid.getValue() + "']",
//				D1TypeBuilder.isIdentifierValid(pid));
//	
//		pid = D1TypeBuilder.buildIdentifier("andABottleOfRum");
//		assertTrue("identifier without spaces should return true.",
//				D1TypeBuilder.isIdentifierValid(pid));
//	
//	}

	@Test
	public void testBuildSubject() {
		String val = "foo";
		Subject s = D1TypeBuilder.buildSubject(val);
		assertTrue("a new object containing the correct value is built",
				s.getValue().equals(val));
	}

//	@Test
//	public void testBuildAccessRuleStringPermission() {
//		fail("Not yet implemented");
//	}
//
//	@Test
//	public void testBuildAccessRuleStringPermissionArray() {
//		fail("Not yet implemented");
//	}

//	@Test
//	public void testBuildPerson() {
//		fail("Not yet implemented");
//	}

//	@Test
//	public void testBuildMinimalSystemMetadata() {
//		fail("Not yet implemented");
//	}

	@Test
	public void testCloneSystemMetadata() throws NoSuchAlgorithmException 
	{
		SystemMetadata orig = new SystemMetadata();
		orig.setAccessPolicy(AccessUtil.createSingleRuleAccessPolicy(
				new String[]{"foo"},
				new Permission[]{Permission.READ}));
		orig.setArchived(true);
		orig.setAuthoritativeMemberNode(D1TypeBuilder.buildNodeReference("myNodeRef"));
		orig.setChecksum(ChecksumUtil.checksum("lalalalalala".getBytes(), "MD5"));
		Date t0 = new Date();
		orig.setDateSysMetadataModified(t0);
		orig.setDateUploaded(t0);
		orig.setFormatId(D1TypeBuilder.buildFormatIdentifier("myFormat"));
		orig.setIdentifier(D1TypeBuilder.buildIdentifier("myID"));
		orig.setObsoletedBy(D1TypeBuilder.buildIdentifier("theNewThing"));
		orig.setObsoletes(D1TypeBuilder.buildIdentifier("theOldThing"));
		orig.setOriginMemberNode(D1TypeBuilder.buildNodeReference("myNodeRef"));
		
		Replica rep = new Replica();
		rep.setReplicationStatus(ReplicationStatus.REQUESTED);
		rep.setReplicaMemberNode(D1TypeBuilder.buildNodeReference("myOtherNodeRef"));
		rep.setReplicaVerified(t0);
		orig.addReplica(rep);
		
		orig.setRightsHolder(D1TypeBuilder.buildSubject("me"));
		orig.setSubmitter(D1TypeBuilder.buildSubject("me"));
		orig.setSize(BigInteger.TEN);
		orig.setSerialVersion(BigInteger.ONE);
		
		SystemMetadata clone = D1TypeBuilder.cloneSystemMetadata(orig);
		
		assertTrue("clone contains different property instance for AP",
				clone.getAccessPolicy().hashCode() != orig.getAccessPolicy().hashCode());

		assertTrue(clone.getAccessPolicy().sizeAllowList() == 1);
		AccessRule ar = clone.getAccessPolicy().getAllow(0);
		assertTrue(ar.sizeSubjectList() ==1 );
		assertTrue(ar.sizePermissionList() == 1);
		assertTrue(ar.getSubject(0).getValue().equals("foo"));
		assertTrue(ar.getPermission(0).equals(Permission.READ));
			
		AccessRule rule = D1TypeBuilder.buildAccessRule("hi", Permission.WRITE);
		clone.getAccessPolicy().addAllow(rule);
		assertTrue(orig.getAccessPolicy().sizeAllowList() == 1);

		
		assertTrue("archived values match", clone.getArchived().booleanValue() == orig.getArchived().booleanValue());
		clone.setArchived(false);
		assertFalse("archived values diverge", clone.getArchived() == orig.getArchived());
		
		assertTrue("AutMN value match", clone.getAuthoritativeMemberNode().getValue() == orig.getAuthoritativeMemberNode().getValue());
		clone.setAuthoritativeMemberNode(D1TypeBuilder.buildNodeReference("cloneAuthMN"));
		assertFalse("AuthMN values diverge", clone.getAuthoritativeMemberNode().getValue() == orig.getAuthoritativeMemberNode().getValue());

		assertTrue("modified date is the same", clone.getDateSysMetadataModified().equals(orig.getDateSysMetadataModified()));		
		clone.setDateSysMetadataModified(new Date());
		assertFalse("mod dates diverge", clone.getDateSysMetadataModified().equals(orig.getDateSysMetadataModified()));

		assertTrue("upload date is the same", clone.getDateUploaded().equals(orig.getDateUploaded()));		
		clone.setDateUploaded(new Date());
		assertFalse("upload dates diverge", clone.getDateUploaded().equals(orig.getDateUploaded()));

		assertTrue("identifier is the same", clone.getIdentifier().getValue().equals(orig.getIdentifier().getValue()));		
		clone.setIdentifier(D1TypeBuilder.buildIdentifier("oink"));
		assertFalse("identifiers diverge", clone.getIdentifier().getValue().equals(orig.getIdentifier().getValue()));

		assertTrue("submitter is the same", clone.getSubmitter().getValue().equals(orig.getSubmitter().getValue()));		
		clone.setSubmitter(D1TypeBuilder.buildSubject("oink"));
		assertFalse("submitters diverge", clone.getSubmitter().getValue().equals(orig.getSubmitter().getValue()));

		
	}

	@Test
	public void testCloneIdentifier() {
		Identifier orig = D1TypeBuilder.buildIdentifier("foo");
		Identifier clone = D1TypeBuilder.cloneIdentifier(orig);
		assertTrue(orig.getValue().equals(clone.getValue()));
		clone.setValue("bar");
		assertTrue("original value still 'foo'", orig.getValue().equals("foo"));
	}

	@Test
	public void testCloneSubject() {
		Subject orig = D1TypeBuilder.buildSubject("foo");
		Subject clone = D1TypeBuilder.cloneSubject(orig);
		assertTrue(orig.getValue().equals(clone.getValue()));
		clone.setValue("bar");
		assertTrue("original value still 'foo'", orig.getValue().equals("foo"));
	}

	@Test
	public void testCloneFormatIdentifier() {
		ObjectFormatIdentifier orig = D1TypeBuilder.buildFormatIdentifier("foo");
		ObjectFormatIdentifier clone = D1TypeBuilder.cloneFormatIdentifier(orig);
		assertTrue(orig.getValue().equals(clone.getValue()));
		clone.setValue("bar");
		assertTrue("original value still 'foo'", orig.getValue().equals("foo"));
		
	}

	@Test
	public void testCloneNodeReference() {
		NodeReference orig = D1TypeBuilder.buildNodeReference("foo");
		NodeReference clone = D1TypeBuilder.cloneNodeReference(orig);
		assertTrue(orig.getValue().equals(clone.getValue()));
		clone.setValue("bar");
		assertTrue("original value still 'foo'", orig.getValue().equals("foo"));
		
	}

	@Test
	public void testCloneChecksum() {
		Checksum orig = new Checksum();
		orig.setAlgorithm("xxx");
		orig.setValue("foo");
		
		Checksum clone = D1TypeBuilder.cloneChecksum(orig);
		assertTrue(orig.getValue().equals(clone.getValue()));
		clone.setValue("bar");
		assertTrue("original value still 'foo'", orig.getValue().equals("foo"));

		assertTrue(orig.getAlgorithm().equals(clone.getAlgorithm()));
		clone.setAlgorithm("qwerty");
		assertTrue("original checksum algorithm still 'xxx'", orig.getAlgorithm().equals("xxx"));
	
	}
}
