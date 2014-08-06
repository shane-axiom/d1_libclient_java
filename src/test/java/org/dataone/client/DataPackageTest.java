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


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dataone.ore.ResourceMapFactory;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.types.v1.Identifier;
import org.dspace.foresite.OREException;
import org.dspace.foresite.OREParserException;
import org.dspace.foresite.ORESerialiserException;
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
		
		Identifier packageId = new Identifier();
		packageId.setValue("package.1.1");
		DataPackage dataPackage = new DataPackage(packageId);
		 
		//Create the predicate URIs and NS
		String provNS = "http://www.w3.org/ns/prov#";
		String wasDerivedFromURI = provNS + "wasDerivedFrom";
		String usedURI = provNS + "used";
		String wasGeneratedByURI = provNS + "wasGeneratedBy";
		String generatedURI = provNS + "generated";
		String wasInformedByURI = provNS + "wasInformedBy";
		
		//Create the derived resources
		//A resource map
		Identifier resourceMapId = new Identifier();
		resourceMapId.setValue("resouceMap.1.1");
		//Metadata
		Identifier metadataId = new Identifier();
		metadataId.setValue("meta.1.1");
		//One derived data 
		Identifier dataId = new Identifier();
		dataId.setValue("data.1.1");
		//Two activities (e.g. scripts)
		Identifier drawActivityId = new Identifier();
		drawActivityId.setValue("drawActivity.1.1");
		Identifier composeActivityId = new Identifier();
		composeActivityId.setValue("composeActivity.1.1");
		//A figure/image
		Identifier imgId = new Identifier();
		imgId.setValue("img.1.1");
		
		//Map the objects in the data package
		List<Identifier> dataIds = new ArrayList<Identifier>();
		dataIds.add(dataId);
		dataIds.add(drawActivityId);
		dataIds.add(composeActivityId);
		dataIds.add(imgId);	
		dataPackage.insertRelationship(metadataId, dataIds);
		
		//Create the primary resources
		Identifier primaryDataId = new Identifier();
		primaryDataId.setValue("primaryData.1.1");
		Identifier primaryDataId2 = new Identifier();
		primaryDataId2.setValue("primaryData.2.1"); 
				
		//Create a list of ids of the primary data resources
		List<Identifier> primaryDataIds = new ArrayList<Identifier>();
		primaryDataIds.add(primaryDataId);
		primaryDataIds.add(primaryDataId2);
		
		//Create lists of the items to use in the triples
		List<Identifier> dataIdList = new ArrayList<Identifier>();
		dataIdList.add(dataId);
		List<Identifier> activityIdList = new ArrayList<Identifier>();
		activityIdList.add(drawActivityId);
		List<Identifier> composeActivityIdList = new ArrayList<Identifier>();
		composeActivityIdList.add(composeActivityId);
		List<Identifier> primaryData1IdList = new ArrayList<Identifier>();
		primaryData1IdList.add(primaryDataId);
		List<Identifier> primaryData2IdList = new ArrayList<Identifier>();
		primaryData2IdList.add(primaryDataId2);
		
		//---- wasDerivedFrom ----
		dataPackage.insertRelationship(dataId, primaryDataIds, provNS, wasDerivedFromURI);
		dataPackage.insertRelationship(imgId, dataIdList, provNS, wasDerivedFromURI);
		
		//---- wasGeneratedBy ----
		dataPackage.insertRelationship(imgId, activityIdList, provNS, wasGeneratedByURI);
		dataPackage.insertRelationship(dataId, composeActivityIdList, provNS, wasGeneratedByURI);
		
		//---- wasInformedBy ----
		dataPackage.insertRelationship(drawActivityId, composeActivityIdList, provNS, wasInformedByURI);
		
		//---- used ----
		dataPackage.insertRelationship(drawActivityId, dataIdList, provNS, usedURI);
		dataPackage.insertRelationship(composeActivityId, primaryData1IdList, provNS, usedURI);
		//Test inserting another relationship with the same subject and predicate but a new object
		dataPackage.insertRelationship(composeActivityId, primaryData2IdList, provNS, usedURI);
		
		//Create the resourceMap
		ResourceMap resourceMap = dataPackage.getMap();
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
