package org.dataone.ore;

import static org.junit.Assert.*;

import java.io.FileWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dataone.client.v1.itk.DataPackage;
import org.dataone.client.v1.types.D1TypeBuilder;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.vocabulary.PROV;
import org.dspace.foresite.OREException;
import org.dspace.foresite.ORESerialiserException;
import org.dspace.foresite.Predicate;
import org.dspace.foresite.ResourceMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ProvResourceMapBuilderTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testCreateResourceMapWithProvONE() {
        
	    
	    System.out.println("***************  testCreateResourceMapWithProvONE  ******************");

	    // The CN resolve URI base:
	    String D1_URI_PREFIX = Settings.getConfiguration()
	            .getString("D1Client.CN_URL","https://cn-dev.test.dataone.org/cn") + "/v1/resolve/";

        // metadata 
        Identifier metadataId = D1TypeBuilder.buildIdentifier("metadata.1.1");

        // data
        Identifier dataId = D1TypeBuilder.buildIdentifier("data.1.1");       
        List<Identifier> dataIds = new ArrayList();
        dataIds.add(dataId);

        // resource map
        Identifier packageId = D1TypeBuilder.buildIdentifier("resourceMap.1.1");
        DataPackage dataPackage = new DataPackage(packageId);

        // add data/metadata
        dataPackage.insertRelationship(metadataId, dataIds);

        // prov relationships

        URI subjectId = null;
        URI objectId = null;
        List<URI> objectIds = new ArrayList<URI>();
        Predicate predicate = null;

        /* used */ 
        try {
            // subject (TODO: this should probably be a blank node, figure that out)
            subjectId = new URI(D1_URI_PREFIX + "execution.1.1");                
            // predicate
            predicate = PROV.predicate("used");
            // object
            objectIds.clear();
            objectId = new URI(D1_URI_PREFIX + "user.1.1");
            objectIds.add(objectId);

            // add used
            // prov
            dataPackage.insertRelationship(subjectId, predicate, objectIds);
            
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            
        } catch (URISyntaxException e) {
            e.printStackTrace();
                        
        }

        /* wasAssociatedWith */ 
        // subject
        try {
            subjectId = new URI(D1_URI_PREFIX + "execution.1.1");                
            // predicate
            predicate = PROV.predicate("wasAssociatedWith");
            // object
            objectIds.clear();
            objectId = new URI(D1_URI_PREFIX + "data.1.1");
            objectIds.add(objectId);
                    
            
            // add wasAssociatedWith
            // prov
            dataPackage.insertRelationship(subjectId, predicate, objectIds);
            
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            
        } catch (URISyntaxException e) {
            fail( e.getMessage());
            
        }
        
        // Print it
        try {
            System.out.println(dataPackage.serializePackage());
        } catch (OREException e) {
            fail( e.getMessage());
            
        } catch (URISyntaxException e) {
            fail( e.getMessage());
            
        } catch (ORESerialiserException e) {
            fail( e.getMessage());
            
        }

	}
	
	@Test
	public void testCreateResourceMapWithPROV() {
		System.out.println("***************  testCreateResourceMapWithPROV  ******************");
		
		try {
			//Create the derived resources
			//A resource map
			Identifier resourceMapId = new Identifier();
			resourceMapId.setValue("doi://1234/AA/map.1.1");
			//Metadata
			Identifier metadataId = new Identifier();
			metadataId.setValue("doi://1234/AA/meta.1.1");
			//One derived data file
			Identifier dataId = new Identifier();
			dataId.setValue("doi://1234/AA/data.1.1");
			//Two activity files (e.g. scripts)
			Identifier drawActivityId = new Identifier();
			drawActivityId.setValue("doi://1234/AA/drawActivity.1.1");
			Identifier composeActivityId = new Identifier();
			composeActivityId.setValue("doi://1234/AA/composeActivity.1.1");
			//A graph/chart/visualization file
			Identifier imgId = new Identifier();
			imgId.setValue("doi://1234/AA/img.1.1");
			
			//Map the files and create the resource map
			List<Identifier> idsToMap = new ArrayList<Identifier>();
			idsToMap.add(dataId);
			idsToMap.add(drawActivityId);
			idsToMap.add(composeActivityId);
			idsToMap.add(imgId);
			Map<Identifier, List<Identifier>> idMap = new HashMap<Identifier, List<Identifier>>();
			idMap.put(metadataId, idsToMap);			
			ResourceMapFactory rmf = ResourceMapFactory.getInstance();
			ProvResourceMapBuilder provBuilder = ProvResourceMapBuilder.getInstance();
			ResourceMap resourceMap = rmf.createResourceMap(resourceMapId, idMap);
			assertNotNull(resourceMap);
			
			//Create the primary resources
			// Two data files
			Identifier primaryDataId = new Identifier();
			primaryDataId.setValue("doi://1234/AA/primaryData.1.1");
			Identifier primaryDataId2 = new Identifier();
			primaryDataId2.setValue("doi://1234/AA/primaryData.2.1"); 
			
			//Create a list of ids of the primary data resources
			List<Identifier> primaryDataIds = new ArrayList<Identifier>();
			primaryDataIds.add(primaryDataId);
			primaryDataIds.add(primaryDataId2);
			
			//---- wasDerivedFrom ----
			//Map these ids to the id of the data they are derived from
			Map<Identifier, List<Identifier>> wasDerivedFromMap = new HashMap<Identifier, List<Identifier>>();
			wasDerivedFromMap.put(dataId, primaryDataIds);
			resourceMap = provBuilder.addWasDerivedFrom(resourceMap, wasDerivedFromMap);
			
			//---- wasGeneratedBy ----
			//Map entity ids to the activities they were generated by
			Map<Identifier, List<Identifier>> wasGeneratedByMap = new HashMap<Identifier, List<Identifier>>();
			List<Identifier> activityIds = new ArrayList<Identifier>();
			activityIds.add(drawActivityId);
			wasGeneratedByMap.put(imgId, activityIds);
			//rmf.addWasGeneratedBy(resourceMap, wasGeneratedByMap);
			resourceMap = provBuilder.addWasGeneratedBy(resourceMap, imgId, drawActivityId);
			resourceMap = provBuilder.addWasGeneratedBy(resourceMap, dataId, composeActivityId);
			
			//---- wasInformedBy ----
			resourceMap = provBuilder.addWasInformedBy(resourceMap, drawActivityId, composeActivityId);
			
			//---- used ----
			Map<Identifier, List<Identifier>> usedMap = new HashMap<Identifier, List<Identifier>>();
			usedMap.put(composeActivityId, primaryDataIds);
			resourceMap = provBuilder.addUsed(resourceMap, drawActivityId, dataId);
			resourceMap = provBuilder.addUsed(resourceMap, usedMap);
			
			//Create an XML document with the serialized RDF
			FileWriter fw = new FileWriter("target/testCreateResourceMapWithPROV.xml");
			String rdfXml = ResourceMapFactory.getInstance().serializeResourceMap(resourceMap);
			assertNotNull(rdfXml);
			//Print it
			fw.write(rdfXml);
			fw.flush();
			fw.close();
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

}
