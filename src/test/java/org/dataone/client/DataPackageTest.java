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
import java.util.ArrayList;
import java.util.List;

import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Subject;
import org.junit.Before;
import org.junit.Test;

public class DataPackageTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testSerializePackage()
	{
		Identifier packageId = D1TypeBuilder.buildIdentifier("myPackageID");
		DataPackage dataPackage = new DataPackage(packageId);
		Identifier metadataId = D1TypeBuilder.buildIdentifier("myMetadataID");
		List<Identifier> dataIds = new ArrayList<Identifier>();
		dataIds.add(D1TypeBuilder.buildIdentifier("myDataID"));
		dataPackage.insertRelationship(metadataId, dataIds);
		String resourceMapText = dataPackage.serializePackage();
//		assertNotNull(resourceMapText);
		System.out.println("the resource map is: " + resourceMapText);
		
		
		
		
	}
	
}
