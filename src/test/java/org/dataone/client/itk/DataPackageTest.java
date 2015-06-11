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


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dataone.client.exception.ClientSideException;
import org.dataone.client.v1.itk.DataPackage;
import org.dataone.client.v1.types.D1TypeBuilder;
import org.dataone.configuration.Settings;
import org.dataone.ore.ResourceMapFactory;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.types.v1.Identifier;
import org.dataone.vocabulary.PROV;
import org.dspace.foresite.OREException;
import org.dspace.foresite.OREParserException;
import org.dspace.foresite.ORESerialiserException;
import org.dspace.foresite.Predicate;
import org.dspace.foresite.ResourceMap;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

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
	
	@Test
	public void testInsertRelationship() throws OREException, ORESerialiserException, URISyntaxException, IOException
	{
		System.out.println("***************  testInsertRelationship  ******************");

		String D1_URI_PREFIX = 
		    Settings.getConfiguration().getString("D1Client.CN_URL", 
		            "https://cn-dev.test.dataone.org/cn") + "/v1/resolve/";

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
	
}
