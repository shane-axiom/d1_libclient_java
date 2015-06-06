package org.dataone.ore;

import static org.junit.Assert.*;

import java.io.FileWriter;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.dataone.client.v1.itk.DataPackage;
import org.dataone.client.v1.types.D1TypeBuilder;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.vocabulary.PROV;
import org.dataone.vocabulary.ProvONE;
import org.dataone.vocabulary.ProvONE_V1;
import org.dspace.foresite.OREException;
import org.dspace.foresite.ORESerialiserException;
import org.dspace.foresite.Predicate;
import org.dspace.foresite.ResourceMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Selector;
import com.hp.hpl.jena.rdf.model.SimpleSelector;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.vocabulary.RDF;

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

        URI subject = null;
        URI object = null;
        List<URI> objects = new ArrayList<URI>();
        Predicate predicate = null;

        /* prov:Activity (ProvONE:Execution) */ 
        try {
            // Execution used
            // subject (TODO: this should probably be a blank node, figure that out)
            subject = new URI(D1_URI_PREFIX + "execution.1.1");                
            // predicate
            predicate = PROV.predicate("used");
            // object
            objects.clear();
            object = new URI(D1_URI_PREFIX + "data.1.1");
            objects.add(object);
            dataPackage.insertRelationship(subject, predicate, objects);
            
            // Execution type
            predicate = asPredicate(RDF.type, "rdf");
            objects.clear();
            object = new URI(ProvONE.Execution.getURI());
            objects.add(object);
            dataPackage.insertRelationship(subject, predicate, objects);

            // Data type
            subject = new URI(D1_URI_PREFIX + "data.1.1");
            predicate = asPredicate(RDF.type, "rdf");
            objects.clear();
            object = new URI(ProvONE.Data.getURI());
            objects.add(object);
            dataPackage.insertRelationship(subject, predicate, objects);
            
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            
        } catch (URISyntaxException e) {
            e.printStackTrace();
                        
        }

        /* prov:wasAssociatedWith */ 
        try {
            // Execution wasAssociatedWith
            subject = new URI(D1_URI_PREFIX + "execution.1.1");                
            predicate = PROV.predicate("wasAssociatedWith");
            objects.clear();
            object = new URI(D1_URI_PREFIX + "user.1.1");
            objects.add(object);            
            dataPackage.insertRelationship(subject, predicate, objects);

            // User type
            subject = new URI(D1_URI_PREFIX + "user.1.1");
            predicate = asPredicate(RDF.type, "rdf");
            objects.clear();
            object = new URI(ProvONE.User.getURI());
            objects.add(object);
            dataPackage.insertRelationship(subject, predicate, objects);

        } catch (Exception e) {
            e.printStackTrace();            
            fail( e.getMessage());
            
        }

        // Validate the expected triples
        String rdfXML = "";
        try {
            rdfXML = dataPackage.serializePackage();
            System.out.println(rdfXML); // Print it
            
            Model rdfModel = ModelFactory.createDefaultModel();
            InputStream inputStream = IOUtils.toInputStream(rdfXML, "UTF-8");
            rdfModel.read(inputStream, null);
            Resource subjectResource = null;
            Property property = null;
            Resource objectResource = null;
            Selector selector = null;
            StmtIterator statements = null;
            
            // Test for [execution.1.1 @prov:used data.1.1]
            subjectResource = rdfModel.createResource(D1_URI_PREFIX + "execution.1.1");
            property = rdfModel.createProperty(PROV.namespace, "used");
            objectResource = rdfModel.createResource(D1_URI_PREFIX + "data.1.1");
            selector = getSimpleSelector(subjectResource, property,
                    objectResource);
            statements = rdfModel.listStatements(selector);
            
            assertTrue(statements.hasNext());
            
            // Test for [execution.1.1 @rdf:type provone:Execution]
            subjectResource = rdfModel.createResource(D1_URI_PREFIX + "execution.1.1");
            property = rdfModel.createProperty(RDF.getURI(), RDF.type.getLocalName());
            objectResource = rdfModel.createResource(ProvONE.namespace + "Execution");
            selector = getSimpleSelector(subjectResource, property,
                    objectResource);
            statements = rdfModel.listStatements(selector);
            
            assertTrue(statements.hasNext());

            // Test for [data.1.1 @rdf:type provone:Data]
            subjectResource = rdfModel.createResource(D1_URI_PREFIX + "data.1.1");
            property = rdfModel.createProperty(RDF.getURI(), RDF.type.getLocalName());
            objectResource = rdfModel.createResource(ProvONE.namespace + "Data");
            selector = getSimpleSelector(subjectResource, property,
                    objectResource);
            statements = rdfModel.listStatements(selector);
            
            assertTrue(statements.hasNext());

            // Test for [data.1.1 @rdf:type provone:Data]
            subjectResource = rdfModel.createResource(D1_URI_PREFIX + "data.1.1");
            property = rdfModel.createProperty(RDF.getURI(), RDF.type.getLocalName());
            objectResource = rdfModel.createResource(ProvONE.namespace + "Data");
            selector = getSimpleSelector(subjectResource, property,
                    objectResource);
            statements = rdfModel.listStatements(selector);
            
            assertTrue(statements.hasNext());
            
            // Test for [execution.1.1 @prov:wasAssociatedWith user.1.1]
            subjectResource = rdfModel.createResource(D1_URI_PREFIX + "execution.1.1");
            property = rdfModel.createProperty(PROV.namespace, "wasAssociatedWith");
            objectResource = rdfModel.createResource(D1_URI_PREFIX + "user.1.1");
            selector = getSimpleSelector(subjectResource, property,
                    objectResource);
            statements = rdfModel.listStatements(selector);
            
            assertTrue(statements.hasNext());

            // Test for [user.1.1 @rdf:type provone:User]
            subjectResource = rdfModel.createResource(D1_URI_PREFIX + "user.1.1");
            property = rdfModel.createProperty(RDF.getURI(), RDF.type.getLocalName());
            objectResource = rdfModel.createResource(ProvONE.namespace + "User");
            selector = getSimpleSelector(subjectResource, property,
                    objectResource);
            statements = rdfModel.listStatements(selector);
            
            assertTrue(statements.hasNext());
            
        } catch (Exception e) {
            fail( e.getMessage());
            
        }

        
	}

    /*
     * Create a statement selector to query the Jena model to validate statements
     * 
     * @param subjectResource
     * @param property
     * @param objectResource
     * @return
     */
    private Selector getSimpleSelector(Resource subjectResource,
            Property property, Resource objectResource) {
        Selector selector = new SimpleSelector(subjectResource, property, objectResource);
        return selector;
    }

    /*
     * Given a Jena Property and namespace prefix, create an ORE Predicate. This allows
     * us to use the Jena vocabularies
     * 
     * @param predicate
     * @throws URISyntaxException
     */
    private Predicate asPredicate(Property property, String prefix)
            throws URISyntaxException {
        Predicate predicate = new Predicate();
        predicate.setName(property.getLocalName());
        predicate.setNamespace(property.getNameSpace());
        if ( prefix != null || ! prefix.isEmpty() ) {
            predicate.setPrefix(prefix);
            
        }
        predicate.setURI(new URI(property.getURI()));
        
        return predicate;
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
			ProvResourceMapBuilder provBuilder = new ProvResourceMapBuilder();
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
