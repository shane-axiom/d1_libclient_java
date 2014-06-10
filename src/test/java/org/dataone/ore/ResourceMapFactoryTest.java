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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.lang.StringUtils;
import org.dataone.client.D1TypeBuilder;
import org.dataone.client.DataPackage;
import org.dataone.service.types.v1.Identifier;
import org.dspace.foresite.OREException;
import org.dspace.foresite.OREParserException;
import org.dspace.foresite.ORESerialiserException;
import org.dspace.foresite.Predicate;
import org.dspace.foresite.ResourceMap;
import org.dspace.foresite.Triple;
import org.dspace.foresite.TripleSelector;
import org.dspace.foresite.jena.OREResourceJena;
import org.junit.Test;

import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

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
			System.out.println(e.getMessage());
			fail();
		}
	}
	
	@Test
	public void testCreateResourceMapWithPROV() {
		System.out.println("***************  testCreateResourceMapWithPROV  ******************");
		
		try {
			//Create the derived resources
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
			
			//Create the primary resources
			Identifier primaryResourceMapId = new Identifier();
			primaryResourceMapId.setValue("doi://1234/AA/primaryMap.1.1");		
			Identifier primaryMetadataId = new Identifier();
			primaryMetadataId.setValue("doi://1234/AA/primaryMeta.1.1");
			Identifier primaryDataId = new Identifier();
			primaryDataId.setValue("doi://1234/AA/primaryData.1.1");
			List<Identifier> resourceMaps = new ArrayList<Identifier>();
			resourceMaps.add(primaryResourceMapId);
			Map<Identifier, List<Identifier>> primaryIdMap = new HashMap<Identifier, List<Identifier>>();
			primaryIdMap.put(primaryDataId, resourceMaps);
			
			//
			rmf.addWasDerivedFrom(resourceMap, primaryDataId, dataId);
			
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
			System.out.println(e.getMessage());
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
//		String resourceMaps = "/D1shared/resourceMaps/libclient_python_example_2013_11_5.xml";
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
	
	
//	@Test 
	public void testParseResourceMap() 
	{
		String[] files = new String[]{
				"libclient_python_example_2013_04_16.xml",
				"libclient_python_example_2013_11_5.xml",
				"libclient_java_example_2013_04_15.xml",
				"missingIsDescribedByTriple.xml",
//				"test_ResMap_isByOnly_5.xml",
//				"test_ResMap_isByOnly_5000.xml",
//				"test_ResMap_mixedDocDocBy_10.xml",
//				"test_ResMap_DocsOnly_10.xml"
//				"merritt_literal_describes_object.xml",
//				"merritt_literal_documents_object.xml",
//				"merritt_literal_aggregates_object.xml",
				"merritt_fixed_describes_object.xml"
				};
		
		boolean threwException = false;
		for (String f : files) {
			try {
				String resourceMap = "/D1shared/resourceMaps/" + f;
				System.out.println(resourceMap);
				InputStream is = this.getClass().getResourceAsStream(resourceMap);
				CountingInputStream cis = new CountingInputStream(is);
				Map<Identifier, Map<Identifier,List<Identifier>>> d1rm =
						ResourceMapFactory.getInstance().parseResourceMap(cis);
				showResourceMap(d1rm);
			} catch (Exception e) {
				threwException = true;
				e.printStackTrace();
			}
		}
		if (threwException) {
			fail("One of the resource maps failed to parse");
		}
	}
		
	private void showResourceMap(Map<Identifier, Map<Identifier,List<Identifier>>> d1rm) {
		for ( Identifier packId: d1rm.keySet()) {
			System.out.println(packId.getValue());
			for (Identifier metadata: d1rm.get(packId).keySet()) {
				System.out.println("   " + metadata.getValue());
				for (Identifier dataId : d1rm.get(packId).get(metadata)) {
					System.out.println("     " + dataId.getValue());
				}
			}
		}
		System.out.println("");
	}
	
	
	
	
	
	
//	@Test
	public void testValidateResourceMap_Valid_PythonGenerated() 
	throws UnsupportedEncodingException, OREException, URISyntaxException, OREParserException 
	{
		testValidateResourceMap("libclient_python_example_2013_04_16.xml", 
				"ResourceMap should validate", true);
	}
	
//	@Test
	public void testValidateResourceMap_Valid_JavaGenerated() 
	throws UnsupportedEncodingException, OREException, URISyntaxException, OREParserException 
	{
		testValidateResourceMap("libclient_java_example_2013_04_15.xml", 
				"ResourceMap should validate", true);
	}
	
//	@Test
	public void testValidateResourceMap_MissingIdentifier() 
	throws UnsupportedEncodingException, OREException, URISyntaxException, OREParserException 
	{
		testValidateResourceMap("missingIdentifier.xml", 
				"ResourceMap missing an identifier triple should fail", false);
	}
	
//	@Test
	public void testValidateResourceMap_nonStandardAggregation() 
	throws UnsupportedEncodingException, OREException, URISyntaxException, OREParserException 
	{
		testValidateResourceMap("nonStandardAggregationResourceURI.xml", 
				"ResourceMap without standard aggregation URI should fail", false);
	}
 
//	@Test
	public void testValidateResourceMap_notCnResolveResources() 
	throws UnsupportedEncodingException, OREException, URISyntaxException, OREParserException 
	{
		testValidateResourceMap("nonCnResolveURIs.xml",
				"ResourceMap without CN_Read.resolve Resources should fail",false);
	}
	
//	@Test
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
	
	
//	@Test 
	public void testRDFReasoningIsDescribedBy() 
	throws OREException, URISyntaxException, OREParserException, IOException 
	{

		
		InputStream is = this.getClass().getResourceAsStream("/D1shared/resourceMaps/missingIsDescribedByTriple.xml");

		ResourceMap  rm = ResourceMapFactory.getInstance().deserializeResourceMap(is, true);
		
		// create the CITO:isDocumentedBy predicate
		Predicate oreIsDescribedByPred = new Predicate();
		oreIsDescribedByPred.setNamespace("http://www.openarchives.org/ore/terms/");
		oreIsDescribedByPred.setPrefix("ore");
		oreIsDescribedByPred.setName("isDescribedBy");
		oreIsDescribedByPred.setURI(new URI(oreIsDescribedByPred.getNamespace() 
				+ oreIsDescribedByPred.getName()));
		
		// create the CITO:documents predicate
		Predicate oreDescribesPred = new Predicate();
		oreDescribesPred.setNamespace(oreIsDescribedByPred.getNamespace());
		oreDescribesPred.setPrefix(oreIsDescribedByPred.getPrefix());
		oreDescribesPred.setName("describes");
		oreDescribesPred.setURI(new URI(oreDescribesPred.getNamespace() 
				+ oreDescribesPred.getName()));
		
		
		TripleSelector ts = 
        		new TripleSelector(rm.getURI(), oreDescribesPred.getURI(), null);
		List<Triple> triples = rm.listAllTriples(ts);
		System.out.println("describes: number of triples: " + triples.size());
		

		ts = new TripleSelector(null, oreIsDescribedByPred.getURI(), rm.getURI());
		triples = rm.listAllTriples(ts);
		System.out.println("isDescribedBy: number of triples: " + triples.size());

		//		System.out.println(triples.get(0).getPredicate().getName());
		
		
		
		assertTrue("should get an isDescribedBy triple", triples.size() > 0);
		
	}
	
//	@Test
	public void testOREModelReusability() 
	throws UnsupportedEncodingException, OREException, URISyntaxException, OREParserException 
	{
		InputStream is = this.getClass().getResourceAsStream("/D1shared/resourceMaps/missingIsDescribedByTriple.xml");
		ResourceMap  rm = ResourceMapFactory.getInstance().deserializeResourceMap(is, true);
		
		Predicate oreDescribesPred = new Predicate();
		oreDescribesPred.setNamespace("http://www.openarchives.org/ore/terms/");
		oreDescribesPred.setPrefix("ore");
		oreDescribesPred.setName("describes");
		oreDescribesPred.setURI(new URI(oreDescribesPred.getNamespace() 
				+ oreDescribesPred.getName()));
		
		TripleSelector ts = 
        		new TripleSelector(rm.getURI(), oreDescribesPred.getURI(), null);
		List<Triple> triples = rm.listAllTriples(ts);
		System.out.println("describes: number of triples: " + triples.size());
		
		
		is = this.getClass().getResourceAsStream("/D1shared/resourceMaps/libclient_python_example_2013_04_16.xml");
		rm = ResourceMapFactory.getInstance().deserializeResourceMap(is, true);
		
		ts = new TripleSelector(rm.getURI(), oreDescribesPred.getURI(), null);
		triples = rm.listAllTriples(ts);
		System.out.println("describes: number of triples: " + triples.size());
		
		assertTrue("should not get more than one describes statement when reusing" +
				" the cached model",triples.size() == 1);
	
	}
	
	/**
	 * a test to see how large of a resource map is possible to build using DataPackage
	 * (1000 takes 3 seconds, 3000 takes 20 seconds, 10000 takes 5 minutes, 30000 
	 * runs out of memory after 45 minutes :-) ) 
	 * @throws OREException
	 * @throws URISyntaxException
	 * @throws ORESerialiserException
	 * @throws IOException
	 */
//	@Test
/*	public void testCreateHugeResourceMap() 
	throws OREException, URISyntaxException, ORESerialiserException, IOException 
	{
		int count = 10;
		System.out.println("count: " + count);
		
		
		DataPackage dp = new DataPackage(D1TypeBuilder.buildIdentifier("MonolithicPackage"));
		List<Identifier> datum = new LinkedList<Identifier>();
		for (int i=1; i<=count; i++) {
			datum.add(D1TypeBuilder.buildIdentifier("MonolithicData_" + i));
		}
		
		Date now = new Date();
		System.out.println("start build model " + now);
		dp.insertRelationship(D1TypeBuilder.buildIdentifier("MonolithicMetadata"), datum);
	
		ResourceMap rm = ResourceMapFactory.getInstance().createSparseResourceMap(dp.getPackageId(),dp.getMetadataMap());
		
		System.out.println("start serialize: " + new Date());
		String epic = ResourceMapFactory.getInstance().serializeResourceMap(rm);
//		String epic = dp.serializePackage();
		now = new Date();
		System.out.println("start writing to file... " + now);
		FileWriter fw = new FileWriter("/Users/rnahf/Downloads/test_ResMap_DocsOnly_" + count + ".xml");
		fw.write(epic);
		fw.flush();
		fw.close();
		now = new Date();
		System.out.println("Done: " + now);
	}
*/	
	
//	@Test
	public void testDeserializeHugeResourceMap() throws OREException, URISyntaxException, OREParserException, IOException 
	{
//		InputStream is = new FileInputStream("/tmp/monolithicResourceMap.xml");
//		InputStream is = new FileInputStream("/tmp/monolithicSparseRM.xml");
//		InputStream is = new FileInputStream("/tmp/monolithicSparseNoIDRM.xml");
//		InputStream is = new FileInputStream("/tmp/monolithicResourceMap_10000.xml");
//		InputStream is = new FileInputStream("/Users/rnahf/software/tools/perl/sampleRDFs/sampleRDF_000033.xml");
		InputStream is = new FileInputStream("/Users/rnahf/Downloads/test_data_package.xml");
		Date now = new Date();
//		System.out.println(IOUtils.toString(is));
		System.out.println("start: " + now);

		ResourceMap rm = ResourceMapFactory.getInstance().deserializeResourceMap(is, true);
		System.out.println(rm);
		now = new Date();
		System.out.println("middle: " + now);
		
		System.out.println("model size: " + ((OREResourceJena) rm).getModel().size());
		now = new Date();
		System.out.println("end: " + now);
	}
	
	
//	@Test
	public void testCreateNestedResourceMaps() 
	throws OREException, URISyntaxException, ORESerialiserException, IOException 
	{

		Date now = new Date();
		System.out.println("start: " + now);
		DataPackage dp = new DataPackage(D1TypeBuilder.buildIdentifier("ParentPackage"));
		List<Identifier> datum = new LinkedList<Identifier>();

		int count = 10000;
		for (int i=1; i<=count; i++) {
			datum.add(D1TypeBuilder.buildIdentifier("ChildPackage_" + i));
		
			// create the childPackage
			DataPackage chp = new DataPackage(D1TypeBuilder.buildIdentifier("ChildPackage_" + i));

			List<Identifier> childData = new LinkedList<Identifier>();
			childData.add(D1TypeBuilder.buildIdentifier("DataForChild_" + i));
			
			chp.insertRelationship(
					D1TypeBuilder.buildIdentifier("MetadataForChild_" + i),
					childData);
		
			String chReM = chp.serializePackage();
			FileWriter fw = new FileWriter("/tmp/ChildPackage_" + i + ".xml");
			fw.write(chReM);
			fw.flush();
			fw.close();
		}

		dp.insertRelationship(D1TypeBuilder.buildIdentifier("MetadataForAll"), datum);
		String parentPackage = dp.serializePackage();
		now = new Date();
		FileWriter fw = new FileWriter("/tmp/ParentPackage.xml");
		fw.write(parentPackage);
		fw.flush();
		fw.close();


		Model model = ModelFactory.createOntologyModel(
				OntModelSpec.OWL_DL_MEM_RULE_INF, 
				ResourceMapFactory.getInstance().getOREModel());
		
		model.read("file:///tmp/ParentPackage.xml");
		model.read("file:///tmp/ChildPackage_1.xml");
		model.read("file:///tmp/ChildPackage_2.xml");
		model.read("file:///tmp/ChildPackage_3.xml");

	
		//now see what resources the ParentPackage is said to aggregate
		Predicate pred = new Predicate();
		pred.setNamespace("http://www.openarchives.org/ore/terms/");
		pred.setPrefix("ore");
		pred.setName("aggregates");
		pred.setURI(new URI(pred.getNamespace() 
				+ pred.getName()));

		StmtIterator it = model.listStatements(null,ResourceFactory.createProperty(pred.getURI().toString()) , (String)null);
		int c = 1;
		while (it.hasNext()) {
			Statement st = it.nextStatement();
			if (st != null && st.getSubject() != null &&
					st.getSubject().getURI() != null && st.getSubject().getURI().contains("cn-dev")) {
				System.out.println(String.format("%2d.  %30s %30s %30s", 
					c++,
					cleanupURIs(st.getSubject().getURI()),
					cleanupURIs(st.getPredicate().getURI()),
					cleanupURIs(st.getObject().toString())));
			}
		}
		
	}
	
	private String cleanupURIs(String s) {
		return StringUtils.substringAfterLast(s, "/");
	}
	
	
}
