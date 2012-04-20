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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Subject;
import org.junit.Before;
import org.junit.Test;

public class D1ObjectTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testConstructor() throws NoSuchAlgorithmException, NotFound, InvalidRequest, IOException 
	{
		byte[] data = "someData".getBytes();
		D1Object x = new D1Object(D1TypeBuilder.buildIdentifier("foo"),data, "aFormat", "aSubmitter","aNodeRef");
		
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
			D1Object y = new D1Object(D1TypeBuilder.buildIdentifier(""),
					"someData".getBytes(), 
					D1TypeBuilder.buildFormatIdentifier(""), 
					D1TypeBuilder.buildSubject(""),
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
			D1Object y = new D1Object(D1TypeBuilder.buildIdentifier("foo"),
					"someData".getBytes(), 
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
	
}
