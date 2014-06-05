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

package org.dataone.client.itk;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.util.ByteArrayDataSource;

import org.apache.commons.io.IOUtils;
import org.dataone.client.itk.D1Object;
import org.dataone.client.types.AccessPolicyEditor;
import org.dataone.client.types.D1TypeBuilder;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.util.AccessUtil;
import org.dataone.service.util.Constants;
import org.junit.Before;
import org.junit.Test;

public class D1ObjectTest {

	@Before
	public void setUp() throws Exception {
	}

	
	@Test
	public void testConstructor() throws NoSuchAlgorithmException, NotFound, InvalidRequest, IOException 
	{
		DataSource data = new ByteArrayDataSource("someData".getBytes(),null);
		D1Object x = new D1Object(D1TypeBuilder.buildIdentifier("foo"),
				data, 
				D1TypeBuilder.buildFormatIdentifier("aFormat"), D1TypeBuilder.buildSubject("aSubmitter"),
				D1TypeBuilder.buildNodeReference("aNodeRef"));
		
		D1Object y = new D1Object(D1TypeBuilder.buildIdentifier("foo"),data, 
				D1TypeBuilder.buildFormatIdentifier("aFormat"), 
				D1TypeBuilder.buildSubject("aSubmitter"),
				D1TypeBuilder.buildNodeReference("aNodeRef"));
		
		assertEquals(x.getFormatId(),y.getFormatId());
		assertEquals(x.getSystemMetadata().getSubmitter(),y.getSystemMetadata().getSubmitter());
		assertEquals(x.getSystemMetadata().getAuthoritativeMemberNode(),y.getSystemMetadata().getAuthoritativeMemberNode());
		assertEquals(x.getSystemMetadata().getFormatId().getValue(),y.getSystemMetadata().getFormatId().getValue());
	}
	
	
	@Test
	public void testEmptyParameters() throws NoSuchAlgorithmException, NotFound, IOException
	{
		try {
			DataSource data = new ByteArrayDataSource("someData".getBytes(),null);
			D1Object x = new D1Object(D1TypeBuilder.buildIdentifier(""),
					data, 
					D1TypeBuilder.buildFormatIdentifier(""), D1TypeBuilder.buildSubject(""),
					D1TypeBuilder.buildNodeReference(""));
			
			fail("should not have been able to build D1Object with empty-string parameters");
		} catch (InvalidRequest e) {
			System.out.println(e.getDescription());
		}
		
	}
	
	
	@Test
	public void testNullParameters() throws NoSuchAlgorithmException, NotFound, IOException
	{
		try {
			DataSource data = new ByteArrayDataSource("someData".getBytes(),null);
			D1Object x = new D1Object(D1TypeBuilder.buildIdentifier("foo"),
					data, 
					(ObjectFormatIdentifier) null,
					(Subject) null,
					(NodeReference) null);
			
			fail("should not have been able to build D1Object with empty-string parameters");
		} catch (InvalidRequest e) {
			System.out.println(e.getDescription());
		} finally {}
		
	}
	
	
	@Test
	public void testDeprecatedEmptyParameters() throws NoSuchAlgorithmException, NotFound, IOException
	{
		try {
			D1Object y = new D1Object(D1TypeBuilder.buildIdentifier(""),
					"someData".getBytes(), 
					"", 
					"",
					"");
			fail("should not have been able to build D1Object with empty-string parameters");
		} catch (InvalidRequest e) {
			System.out.println(e.getDescription());
		}
		
	}
	
	
	@Test
	public void testDeprecatedNullParameters() throws NoSuchAlgorithmException, NotFound, IOException
	{
		try {
			D1Object y = new D1Object(D1TypeBuilder.buildIdentifier("foo"),
					"someData".getBytes(), 
					(String) null,
					(String) null,
					(String) null);
			fail("should not have been able to build D1Object with empty-string parameters");
		} catch (InvalidRequest e) {
			System.out.println(e.getDescription());
		} finally {}
		
	}
	

	@Test
	public void testSetDataNull() throws NoSuchAlgorithmException, NotFound, InvalidRequest, IOException 
	{
		D1Object z = new D1Object(D1TypeBuilder.buildIdentifier("foooooo"),
				"someData".getBytes(),
				D1TypeBuilder.buildFormatIdentifier("text/csv"),
				D1TypeBuilder.buildSubject("submitterMe"),
				D1TypeBuilder.buildNodeReference("someMN"));

		try {
			z.setData(null);
			fail("should not have been able to setData(null) without throwing an Exception");
		} catch (InvalidRequest e) {
			;
		}
		
		try {
			z.setSystemMetadata(null);
			fail("should not have been able to setSystemMetadata(null) without throwing an Exception");
		} catch (InvalidRequest e) {
			;
		}
	}
	
	
	@Test
	public void testGetAccessPolicyEditor() throws NoSuchAlgorithmException, NotFound, InvalidRequest, IOException
	{
		D1Object d = new D1Object(D1TypeBuilder.buildIdentifier("foooooo"),
				"someData".getBytes(),
				D1TypeBuilder.buildFormatIdentifier("text/csv"),
				D1TypeBuilder.buildSubject("submitterMe"),
				D1TypeBuilder.buildNodeReference("someMN"));
		
		Subject s = D1TypeBuilder.buildSubject("mee-mee-mee");
		try {
			AccessPolicyEditor editor = d.getAccessPolicyEditor();
			editor.setAccess(new Subject[]{s}, Permission.READ);
			editor = null;
		} finally {}
		
		assertTrue(AccessUtil.getPermissionMap( d.getSystemMetadata().getAccessPolicy()).containsKey(s));
	}
	
	
	@Test
	public void testGetAccessPolicyEditor_setPublicAccess() throws NoSuchAlgorithmException, NotFound, InvalidRequest, IOException
	{
		D1Object d = new D1Object(D1TypeBuilder.buildIdentifier("foooooo"),
				"someData".getBytes(),
				D1TypeBuilder.buildFormatIdentifier("text/csv"),
				D1TypeBuilder.buildSubject("submitterMe"),
				D1TypeBuilder.buildNodeReference("someMN"));
		
		AccessPolicyEditor editor = d.getAccessPolicyEditor();
		editor.setPublicAccess();
		
		assertTrue(AccessUtil.getPermissionMap( 
				d.getSystemMetadata().getAccessPolicy()).containsKey(
						D1TypeBuilder.buildSubject(Constants.SUBJECT_PUBLIC
								)));
	}
	
	
	@Test
	public void testGetDataSource() throws NoSuchAlgorithmException, NotFound, InvalidRequest, IOException {
		
		DataSource data = new ByteArrayDataSource("someData".getBytes(),null);
		D1Object d = new D1Object(D1TypeBuilder.buildIdentifier("foooooo"),
				data, 
				D1TypeBuilder.buildFormatIdentifier("text/csv"),
				D1TypeBuilder.buildSubject("submitterMe"),
				D1TypeBuilder.buildNodeReference("someMN"));
		assertTrue(IOUtils.toString(d.getDataSource().getInputStream()).equals("someData"));	
	}
	
	
	@Test
	public void testSetDataSource() throws NoSuchAlgorithmException, NotFound, InvalidRequest, IOException {
		DataSource data = new ByteArrayDataSource("someData".getBytes(),null);
		D1Object d = new D1Object(D1TypeBuilder.buildIdentifier("foooooo"),
				data, 
				D1TypeBuilder.buildFormatIdentifier("text/csv"),
				D1TypeBuilder.buildSubject("submitterMe"),
				D1TypeBuilder.buildNodeReference("someMN"));
		d.setDataSource(new ByteArrayDataSource("someOtherData".getBytes(),null));
		assertTrue(IOUtils.toString(d.getDataSource().getInputStream()).equals("someOtherData"));
		
	}
	
	
	@Test
	public void testFileDataSource() throws NoSuchAlgorithmException, NotFound, InvalidRequest, IOException {
		
		File dataFile = File.createTempFile("d1ObjectTest", "123");		
		
		try {
			FileWriter fw = new FileWriter(dataFile);
			fw.write("someData");
			fw.flush();
			fw.close();

			DataSource data = new FileDataSource( dataFile );

			D1Object d = new D1Object(D1TypeBuilder.buildIdentifier("foooooo"),
					data, 
					D1TypeBuilder.buildFormatIdentifier("text/csv"),
					D1TypeBuilder.buildSubject("submitterMe"),
					D1TypeBuilder.buildNodeReference("someMN"));

			assertTrue(IOUtils.toString(d.getDataSource().getInputStream()).equals("someData"));	
		} finally {
			dataFile.delete();
		}
		}
	
	
}
