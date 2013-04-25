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

package org.dataone.ore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.lang.StringUtils;
import org.dataone.client.MNode;
import org.dataone.service.types.v1.Identifier;
import org.dspace.foresite.OREException;
import org.dspace.foresite.OREParserException;
import org.dspace.foresite.ResourceMap;
import org.junit.Test;

public class ResourceMapFactoryTest {
	
	@Test
	public void testCreateResourceMap() {
		
		try {
			Identifier resourceMapId = new Identifier();
			resourceMapId.setValue("doi://1234/AA/map.1.1");
			Identifier metadataId = new Identifier();
			metadataId.setValue("doi://1234/AA/meta.1.1");
			List<Identifier> dataIds = new ArrayList<Identifier>();
			Identifier dataId = new Identifier();
			dataId.setValue("doi://1234/AA/data.1.1");
			Identifier dataId2 = new Identifier();
			dataId2.setValue("doi://1234/AA/data.2.1");
			dataIds.add(dataId);
			dataIds.add(dataId2);
			Map<Identifier, List<Identifier>> idMap = new HashMap<Identifier, List<Identifier>>();
			idMap.put(metadataId, dataIds);
			ResourceMapFactory rmf = ResourceMapFactory.getInstance();
			ResourceMap resourceMap = rmf.createResourceMap(resourceMapId, idMap);
			assertNotNull(resourceMap);
			String rdfXml = ResourceMapFactory.getInstance().serializeResourceMap(resourceMap);
			assertNotNull(rdfXml);
			System.out.println(rdfXml);
			
			// now put it back in an object
			Map<Identifier, Map<Identifier, List<Identifier>>> retPackageMap = ResourceMapFactory.getInstance().parseResourceMap(rdfXml);
            Identifier retPackageId = retPackageMap.keySet().iterator().next();   
            
            // Package Identifiers should match
            assertEquals(resourceMapId.getValue(), retPackageId.getValue());
            System.out.println("PACKAGEID IS: " + retPackageId.getValue());

            // Get the Map of metadata/data identifiers
            Map<Identifier, List<Identifier>> retIdMap = retPackageMap.get(retPackageId);
            			
			// same size
			assertEquals(idMap.keySet().size(), retIdMap.keySet().size());
			for (Identifier key : idMap.keySet()) {
			    System.out.println("  ORIGINAL: " + key.getValue());
			    List<Identifier> contained = idMap.get(key);
			    for (Identifier cKey : contained) {
		             System.out.println("    CONTAINS: " + cKey.getValue());
			    }
			}
            for (Identifier key : retIdMap.keySet()) {
                System.out.println("  RETURNED: " + key.getValue());
                List<Identifier> contained = idMap.get(key);
                for (Identifier cKey : contained) {
                     System.out.println("    CONTAINS: " + cKey.getValue());
                }
            }

			// same value
			assertEquals(idMap.keySet().iterator().next().getValue(), retIdMap.keySet().iterator().next().getValue());
			
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
//	@Test
	public void resourceMapChecker() 
			throws UnsupportedEncodingException, OREException, URISyntaxException, OREParserException 
	{
		System.out.println("***************  resourceMapChecker  ******************");
//		String resourceMaps = "/D1shared/resourceMaps/EDAC_draft.xml";
//		String resourceMaps = "/D1shared/resourceMaps/rdf-xml-python-a-cn_resolved.xml";
		String resourceMaps = "/D1shared/resourceMaps/rdf-xml-java-1.xml";
		InputStream is = this.getClass().getResourceAsStream(resourceMaps);
		
		Map<Identifier, Map<Identifier, List<Identifier>>> rm = 
				ResourceMapFactory.getInstance().parseResourceMap(is);

		//   	checkTrue(mn.getLatestRequestUrl(),  
		//   	    "packageId matches packageId used to call", rm.containsKey(packageId));

		if (rm != null) {
			Iterator<Identifier> it = rm.keySet().iterator();

			while (it.hasNext()) {
				Identifier pp = it.next();
				System.out.println("package: " + pp.getValue());
			}

			Map<Identifier, List<Identifier>> agg = rm.get(rm.keySet().iterator().next());
			Iterator<Identifier> itt  = agg.keySet().iterator();
			while (itt.hasNext()) {
				Identifier docs = itt.next();
				System.out.println("md: " + docs.getValue());
				//checkTrue("the identifier should start with https://cn.dataone.org/cn/v1/resolve","",true);
				List<Identifier> docd = agg.get(docs);
				for (Identifier id: docd) {
					System.out.println("data: " + id.getValue());
				}
			}
		} else {
			fail("parseResourceMap returned null for file: " + resourceMaps);
		}

	}
	
	@Test
	public void testValidateResourceMap_Valid_PythonGenerated() 
	throws UnsupportedEncodingException, OREException, URISyntaxException, OREParserException 
	{
		testValidateResourceMap("libclient_python_example_2013_04_16.xml", 
				"ResourceMap should validate", true);
	}
	
	@Test
	public void testValidateResourceMap_Valid_JavaGenerated() 
	throws UnsupportedEncodingException, OREException, URISyntaxException, OREParserException 
	{
		testValidateResourceMap("libclient_java_example_2013_04_15.xml", 
				"ResourceMap should validate", true);
	}
	
	@Test
	public void testValidateResourceMap_MissingIdentifier() 
	throws UnsupportedEncodingException, OREException, URISyntaxException, OREParserException 
	{
		testValidateResourceMap("missingIdentifier.xml", 
				"ResourceMap missing an identifier triple should fail", false);
	}
	
	@Test
	public void testValidateResourceMap_nonStandardAggregation() 
	throws UnsupportedEncodingException, OREException, URISyntaxException, OREParserException 
	{
		testValidateResourceMap("nonStandardAggregationResourceURI.xml", 
				"ResourceMap without standard aggregation URI should fail", false);
	}

	@Test
	public void testValidateResourceMap_notCnResolveResources() 
	throws UnsupportedEncodingException, OREException, URISyntaxException, OREParserException 
	{
		testValidateResourceMap("nonCnResolveURIs.xml",
				"ResourceMap without CN_Read.resolve Resources should fail",false);
	}
	
	@Test
	public void testValidateResourceMap_miscodedIdentifier() 
	throws UnsupportedEncodingException, OREException, URISyntaxException, OREParserException 
	{
		testValidateResourceMap("miscodedIdentifier.xml",
				"ResourceMap with miscoded identifier should fail",false);
	}
	
	
	private void testValidateResourceMap(String exampleFile, String failMessage, boolean expectPass) 
	throws UnsupportedEncodingException, OREException, URISyntaxException, OREParserException 
	{
		System.out.println("example file: " + exampleFile);
		String resourceMap = "/D1shared/resourceMaps/" + exampleFile;
		InputStream is = this.getClass().getResourceAsStream(resourceMap);
		CountingInputStream cis = new CountingInputStream(is);
		ResourceMap rm = ResourceMapFactory.getInstance().deserializeResourceMap(cis);

		System.out.println("byte count: " + cis.getByteCount());
		
		List<String> messages = ResourceMapFactory.getInstance().validateResourceMap(rm);
	
		for (String message : messages) {
			System.out.println(message);
		}
		if (expectPass != messages.isEmpty()) {
			fail(String.format("%s (%s):\n%s",
					failMessage,
					exampleFile,
					StringUtils.join(messages, "\n")));
		}
	
	
	}
}
