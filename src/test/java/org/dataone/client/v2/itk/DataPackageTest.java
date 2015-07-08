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

package org.dataone.client.v2.itk;


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.v2.itk.DataPackage;
import org.dataone.client.v1.types.D1TypeBuilder;
import org.dataone.configuration.Settings;
import org.dataone.ore.ResourceMapFactory;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.types.v1.Identifier;
import org.dataone.vocabulary.DC_TERMS;
import org.dataone.vocabulary.PROV;
import org.dataone.vocabulary.ProvONE;
import org.dspace.foresite.OREException;
import org.dspace.foresite.OREParserException;
import org.dspace.foresite.ORESerialiserException;
import org.dspace.foresite.Predicate;
import org.dspace.foresite.ResourceMap;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Selector;
import com.hp.hpl.jena.rdf.model.SimpleSelector;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.vocabulary.RDF;

public class DataPackageTest {

	@Before
	public void setUp() throws Exception {
	}

	@Ignore("test cannot run as a unit test, as the method calls the CN and MNs")
	@Test
	public void testSerializePackage() throws OREException, URISyntaxException, 
	ORESerialiserException, UnsupportedEncodingException, BaseException, OREParserException, ClientSideException
	{
	    Identifier packageId = D1TypeBuilder.buildIdentifier("myPackageID");
	    DataPackage dataPackage = new DataPackage(packageId);
	    Identifier metadataId = D1TypeBuilder.buildIdentifier("myMetadataID");
	    List<Identifier> dataIds = new ArrayList<Identifier>();
	    dataIds.add(D1TypeBuilder.buildIdentifier("myDataID1"));
	    dataIds.add(D1TypeBuilder.buildIdentifier("myDataID2"));
	    dataIds.add(D1TypeBuilder.buildIdentifier("myDataID3"));
	    dataIds.add(D1TypeBuilder.buildIdentifier("myDataID4"));
	    
	    dataPackage.insertRelationship(metadataId, dataIds);
	    
	    Identifier metadataIDa = D1TypeBuilder.buildIdentifier("myMetadataIDa");
	    List<Identifier> dataIdsa = new ArrayList<Identifier>();
	    dataIdsa.add(D1TypeBuilder.buildIdentifier("myDataID1a"));
	    dataIdsa.add(D1TypeBuilder.buildIdentifier("myDataID2a"));
	    dataIdsa.add(D1TypeBuilder.buildIdentifier("myDataID3a"));
	    dataIdsa.add(D1TypeBuilder.buildIdentifier("myDataID4a"));

	    dataPackage.insertRelationship(metadataIDa, dataIdsa);
	    String resourceMapText = dataPackage.serializePackage();
	    assertNotNull(resourceMapText);
	    System.out.println("the resource map is:\n\n " + resourceMapText);
	    
	    DataPackage dp2 = DataPackage.deserializePackage(resourceMapText);
	    assertTrue("deserialized dataPackage should have the original packageId",
		       dp2.getPackageId().equals(packageId));

	}

	/**
	 * tests the insertion of a literal object of an RDF statement
	 */
	@Test
	public void testInsertRelationshipObjectLiteral() {
        System.out.println("***************  testInsertRelationshipObjectLiteral  ******************");

        String D1_URI_PREFIX = Settings.getConfiguration().getString("D1Client.CN_URL", 
                "https://cn-dev.test.dataone.org/cn") + "/v2/resolve/";
            
            Identifier packageId = D1TypeBuilder.buildIdentifier("package.1.1");
            DataPackage dataPackage = new DataPackage(packageId);

            // Add some metadata and data
            try {
                Identifier metadataId = D1TypeBuilder.buildIdentifier("meta.1.1");
                Identifier dataId = D1TypeBuilder.buildIdentifier("data.1.1");
                List<Identifier> data = new ArrayList<Identifier>();
                data.add(dataId);            
                dataPackage.insertRelationship(metadataId, data);
                
                // Now describe the metadata with another literal identifier
                URI metadataURI = new URI(D1_URI_PREFIX + "meta.1.1");
                dataPackage.insertRelationship(metadataURI, DC_TERMS.predicate("identifier"), "another_id.1.1");

                String rdfXML = dataPackage.serializePackage();
                System.out.println(rdfXML);

                // Load the result into a model for reading
                Model rdfModel = ModelFactory.createDefaultModel();
                InputStream inputStream = IOUtils.toInputStream(rdfXML, "UTF-8");
                rdfModel.read(inputStream, null);

                Resource subjectResource = 
                        ModelFactory.createDefaultModel().createResource(
                                D1_URI_PREFIX + "data.1.1").addProperty(
                                DC_TERMS.identifier, "another_id.1.1");

                Statement idStatement = subjectResource.getProperty(DC_TERMS.identifier);
                Selector selector = getSimpleSelector(idStatement.getSubject(), idStatement.getPredicate(), null );
                StmtIterator statements = rdfModel.listStatements(selector);
                
                assertTrue("The resource map should have a node with an identifier 'another_id.1.1'", 
                        statements.hasNext());
                Statement statement = statements.nextStatement();
                assertTrue("The identifier object is a literal value", statement.getObject().isLiteral());
                

            } catch (OREException e) {
                e.printStackTrace();
                fail(e.getMessage());
                
            } catch (URISyntaxException e) {
                e.printStackTrace();
                fail(e.getMessage());
                
            } catch (ORESerialiserException e) {
                e.printStackTrace();
                fail(e.getMessage());

            } catch (IOException e) {
                e.printStackTrace();
                fail(e.getMessage());

            }


	}
	
	/**
	 * Tests the insertion of a blank node subject of an RDF statement
	 */
	@Test
	public void testInsertRelationshipBlankNode() {
        System.out.println("***************  testInsertRelationshipBlankNode  ******************");
        
        String D1_URI_PREFIX = Settings.getConfiguration().getString("D1Client.CN_URL", 
            "https://cn-dev.test.dataone.org/cn") + "/v2/resolve/";
        
        Identifier packageId = D1TypeBuilder.buildIdentifier("package.1.1");
        DataPackage dataPackage = new DataPackage(packageId);
	    
        Predicate type;
        try {
            
            // First add some metadata and data to ensure the graph is connected
            Identifier metadataId = D1TypeBuilder.buildIdentifier("meta.1.1");
            Identifier dataId = D1TypeBuilder.buildIdentifier("data.1.1");
            List<Identifier> data = new ArrayList<Identifier>();
            data.add(dataId);            
            dataPackage.insertRelationship(metadataId, data);
            
            // use the data in some sort of execution, identified by a blank node
            String blankExecutionNodeId = "execution.1.1";
            
            dataPackage.insertRelationship(blankExecutionNodeId, PROV.predicate("used"), 
                    new URI(D1_URI_PREFIX + dataId.getValue()));
            
            // and type the blank node as a provone:Execution 
            type = asPredicate(RDF.type, "rdf");
            dataPackage.insertRelationship(blankExecutionNodeId, type, new URI(ProvONE.Execution.getURI()));
            
            // Print the RDF
            String rdfXML = dataPackage.serializePackage();
            System.out.println(rdfXML);
            
            // Load the result into a model for reading
            Model rdfModel = ModelFactory.createDefaultModel();
            InputStream inputStream = IOUtils.toInputStream(rdfXML, "UTF-8");
            rdfModel.read(inputStream, null);
            Resource subjectResource = null;
            Property property = null;
            Resource objectResource = null;
            Selector selector = null;
            StmtIterator statements = null;
            Statement statement = null;
                        
            // Test for [[blank node] @prov:used data.1.1]
            objectResource = 
                    ModelFactory.createDefaultModel().createResource(D1_URI_PREFIX + "data.1.1");

            selector = getSimpleSelector(null, PROV.used, objectResource);
            statements = rdfModel.listStatements(selector);
            
            assertTrue("The resource map should have a blank node that used data.1.1", 
                    statements.hasNext());
            statement = statements.nextStatement();
            assertTrue("The returned node is blank", statement.getSubject().isAnon());
            Resource subjectFromUsedStatement = statement.getSubject();
            
            // Test for [[blank node] @rdf:type provone:Execution]
            selector = getSimpleSelector(null, RDF.type, ProvONE.Execution);
            statements = rdfModel.listStatements(selector);
            
            assertTrue("The resource map should have a blank node of type provone:Execution", 
                    statements.hasNext());
            statement = statements.nextStatement();
            assertTrue("The returned node is blank", statement.getSubject().isAnon());
            Resource subjectFromTypeStatement = statement.getSubject();
            assertTrue("The returned node should be the same node returned in the previous select", 
                    subjectFromUsedStatement.equals(subjectFromTypeStatement));        
            
        } catch (URISyntaxException e) {
            e.printStackTrace();
            
        } catch (OREException e) {
            e.printStackTrace();
            
        } catch (ORESerialiserException e) {
            e.printStackTrace();
            
        } catch (IOException e) {
            e.printStackTrace();
            
        }
        
	}
	
	@Test
	public void testInsertRelationship() throws OREException, ORESerialiserException, URISyntaxException, IOException
	{
		System.out.println("***************  testInsertRelationship  ******************");

		String D1_URI_PREFIX = 
		    Settings.getConfiguration().getString("D1Client.CN_URL", 
		            "https://cn-dev.test.dataone.org/cn") + "/v2/resolve/";
		
		System.out.println("D1_URI_PREFIX=" + D1_URI_PREFIX);

		Identifier packageId = new Identifier();
		packageId.setValue("package.1.1");
		DataPackage dataPackage = new DataPackage(packageId);
		 
		//Create the predicate URIs and NS
		Predicate wasDerivedFrom = null;
        Predicate used = null;
        Predicate wasGeneratedBy = null;
        Predicate wasInformedBy = null;
        try {
            wasDerivedFrom = PROV.predicate("wasDerivedFrom");
            used = PROV.predicate("used");
            wasGeneratedBy = PROV.predicate("wasGeneratedBy");
            wasInformedBy = PROV.predicate("wasInformedBy");
            
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            
        }
		
		//Create the derived resources
		//A resource map
		Identifier resourceMapId = D1TypeBuilder.buildIdentifier("resouceMap.1.1");
		//Metadata
		Identifier metadataId = D1TypeBuilder.buildIdentifier("meta.1.1");
		//One derived data 
		Identifier dataId = D1TypeBuilder.buildIdentifier("data.1.1");
		URI dataURI = new URI(D1_URI_PREFIX + "data.1.1");
		//Two activities (e.g. scripts)
		Identifier drawActivityId = D1TypeBuilder.buildIdentifier("drawActivity.1.1");
        URI drawActivityURI = new URI(D1_URI_PREFIX + "drawActivity.1.1");
		Identifier composeActivityId = D1TypeBuilder.buildIdentifier("composeActivity.1.1");
        URI composeActivityURI = new URI(D1_URI_PREFIX + "composeActivity.1.1");
		//A figure/image
		Identifier imgId = D1TypeBuilder.buildIdentifier("img.1.1");
        URI imgURI = new URI(D1_URI_PREFIX + "img.1.1");
		
		//Map the objects in the data package
		List<Identifier> dataIds = new ArrayList<Identifier>();
		dataIds.add(dataId);
		dataIds.add(drawActivityId);
		dataIds.add(composeActivityId);
		dataIds.add(imgId);	
		dataPackage.insertRelationship(metadataId, dataIds);
		
		//Create the primary resources
		Identifier primaryDataId = D1TypeBuilder.buildIdentifier("primaryData.1.1");
        URI primaryDataURI = new URI(D1_URI_PREFIX + "primaryData.1.1");
		Identifier primaryDataId2 = D1TypeBuilder.buildIdentifier("primaryData.2.1"); 
        URI primaryDataURI2 = new URI(D1_URI_PREFIX + "primaryData.2.1");
				
		//Create a list of ids of the primary data resources
		List<URI> primaryDataURIs = new ArrayList<URI>();
		primaryDataURIs.add(primaryDataURI);
		primaryDataURIs.add(primaryDataURI2);
		
		//Create lists of the items to use in the triples
		List<URI> dataURIList = new ArrayList<URI>();
		dataURIList.add(dataURI);
		List<URI> activityURIList = new ArrayList<URI>();
		activityURIList.add(drawActivityURI);
		List<URI> composeActivityURIList = new ArrayList<URI>();
		composeActivityURIList.add(composeActivityURI);
		List<URI> primaryData1URIList = new ArrayList<URI>();
		primaryData1URIList.add(primaryDataURI);
		List<URI> primaryData2URIList = new ArrayList<URI>();
		primaryData2URIList.add(primaryDataURI2);
		
		//---- wasDerivedFrom ----
		dataPackage.insertRelationship(dataURI, wasDerivedFrom, primaryDataURIs);
		dataPackage.insertRelationship(imgURI, wasDerivedFrom, dataURIList);
		
		//---- wasGeneratedBy ----
		dataPackage.insertRelationship(imgURI, wasGeneratedBy, activityURIList);
		dataPackage.insertRelationship(dataURI, wasGeneratedBy, composeActivityURIList);
		
		//---- wasInformedBy ----
		dataPackage.insertRelationship(drawActivityURI, wasInformedBy, composeActivityURIList);
		
		//---- used ----
		dataPackage.insertRelationship(drawActivityURI, used, dataURIList);
		dataPackage.insertRelationship(composeActivityURI, used, primaryData1URIList);
		//Test inserting another relationship with the same subject and predicate but a new object
		dataPackage.insertRelationship(composeActivityURI, used, primaryData2URIList);
		
		//Create the resourceMap
		ResourceMap resourceMap = dataPackage.getResourceMap();
		assertNotNull(resourceMap);

		//Create an XML document with the serialized RDF
		FileWriter fw = new FileWriter("target/testCreateResourceMapWithPROV.xml");
		String rdfXml = ResourceMapFactory.getInstance().serializeResourceMap(resourceMap);
		assertNotNull(rdfXml);
		//Print it
		fw.write(rdfXml);
		fw.flush();
		fw.close();
		System.out.println(rdfXml);
		
	}
	
	@Test
	public void testGetDocumentedBy() throws OREException, URISyntaxException, 
	ORESerialiserException, UnsupportedEncodingException, BaseException, OREParserException
	{
	    Identifier packageId = D1TypeBuilder.buildIdentifier("myPackageID");
	    DataPackage dataPackage = new DataPackage(packageId);
	    Identifier metadataId = D1TypeBuilder.buildIdentifier("myMetadataID");
	    List<Identifier> dataIds = new ArrayList<Identifier>();
	    dataIds.add(D1TypeBuilder.buildIdentifier("myDataID1"));
	    dataIds.add(D1TypeBuilder.buildIdentifier("myDataID2"));
	    dataIds.add(D1TypeBuilder.buildIdentifier("myDataID3"));
	    dataIds.add(D1TypeBuilder.buildIdentifier("myDataID4"));
	    
	    dataPackage.insertRelationship(metadataId, dataIds);
	    
	    Identifier metadataIDa = D1TypeBuilder.buildIdentifier("myMetadataIDa");
	    List<Identifier> dataIdsa = new ArrayList<Identifier>();
	    dataIdsa.add(D1TypeBuilder.buildIdentifier("myDataID1a"));
	    dataIdsa.add(D1TypeBuilder.buildIdentifier("myDataID2a"));
	    dataIdsa.add(D1TypeBuilder.buildIdentifier("myDataID3a"));
	    dataIdsa.add(D1TypeBuilder.buildIdentifier("myDataID4a"));

	    dataPackage.insertRelationship(metadataIDa, dataIdsa);

	    Identifier md = dataPackage.getDocumentedBy(D1TypeBuilder.buildIdentifier("myDataID3"));
	    Identifier mda = dataPackage.getDocumentedBy(D1TypeBuilder.buildIdentifier("myDataID2a"));
	    
	    assertTrue("'myMetadataID' should be returned by getDocumentedBy('myDataID3')", md.equals(metadataId));
	    assertTrue("'myMetadataIDa' should be returned by getDocumentedBy('myDataID2a')", mda.equals(metadataIDa));

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

    /*
     * Create a statement selector to query the Jena model to validate statements
     * 
     * @param subjectResource
     * @param property
     * @param objectNode
     * @return
     */
    private Selector getSimpleSelector(Resource subjectResource,
            Property property, RDFNode objectNode) {
        Selector selector = new SimpleSelector(subjectResource, property, objectNode);
        return selector;
    }


}
