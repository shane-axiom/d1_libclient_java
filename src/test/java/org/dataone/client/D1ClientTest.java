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
 */

package org.dataone.client;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.Callable;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.IOUtils;
import org.dataone.eml.DataoneEMLParser;
import org.dataone.eml.EMLDocument;
import org.dataone.eml.EMLDocument.DistributionMetadata;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Checksum;
import org.dataone.service.types.ChecksumAlgorithm;
import org.dataone.service.types.Event;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.Log;
import org.dataone.service.types.LogEntry;
import org.dataone.service.types.Node;
import org.dataone.service.types.NodeList;
import org.dataone.service.types.NodeReference;
import org.dataone.service.types.ObjectFormat;
import org.dataone.service.types.ObjectInfo;
import org.dataone.service.types.ObjectList;
import org.dataone.service.types.Principal;
import org.dataone.service.types.SystemMetadata;
import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IMarshallingContext;
import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentType;
import org.xml.sax.SAXException;



/**
 * Test the DataONE Java client methods.
 * @author Matthew Jones
 */
public class D1ClientTest  {

    String contextUrl = "http://localhost:8080/knb/";
    //String contextUrl = "http://knb-mn.ecoinformatics.org/knb/";
    //String contextUrl = "http://mn-rpw/mn/";
    //String contextUrl = "http://cn-dev.dataone.org/knb/";
    //String contextUrl = "http://cn-ucsb-1.dataone.org/knb/";
    //String contextUrl = "http://cn-unm-1.dataone.org/knb/";
    
    private static final String prefix = "knb:testid:";
    private static final String bogusId = "foobarbaz214";

    private D1Client d1 = null;
    private List<Node> nodeList = null;
    private static String currentUrl;
    //set this to false if you don't want to use the node list to get the urls for 
    //the test.  
    private static boolean useNodeList = false;
    
    private static String watchedLog;
    
    @Rule 
    public ErrorCollector errorCollector = new ErrorCollector();

    @Before
    public void setUp() throws Exception 
    {
        InputStream nodeRegStream = this.getClass().getResourceAsStream("/org/dataone/client/nodeRegistry.xml");
        NodeList nr = null;
        
        
        /*try 
        {
            IBindingFactory bfact = BindingDirectory.getFactory(NodeList.class);
            IUnmarshallingContext uctx = bfact.createUnmarshallingContext();
            nr = (NodeList) uctx.unmarshalDocument(nodeRegStream, null);
        } 
        catch (JiBXException e) 
        {
            e.printStackTrace();
            throw new ServiceFailure("1190", "Failed to deserialize NodeRegistry: " + e.getMessage());
        }   
        
        if(nr != null)
        {
            nodeList = nr.getNodes();
            
        }*/
        
        nodeList = new Vector<Node>();
        Node n1 = new Node();
        Node n2 = new Node();
        Node n3 = new Node();
        Node n4 = new Node();
        n1.setBaseURL("http://knb-mn.ecoinformatics.org/knb/");
        n2.setBaseURL("http://cn-unm-1.dataone.org/knb/");
        n3.setBaseURL("http://cn-ucsb-1.dataone.org/knb/");
        n4.setBaseURL("http://cn-orc-1.dataone.org/knb/");
        nodeList.add(n1);
        nodeList.add(n2);
        nodeList.add(n3);
        nodeList.add(n4);
        
        if(nodeList == null || nodeList.size() == 0 || !useNodeList)
        {
            nodeList = new Vector<Node>();
            Node n = new Node();
            n.setBaseURL(contextUrl);
            nodeList.add(n);
        }
        
        /*System.out.println("nodes to test on:");
        for(int i=0; i<nodeList.size(); i++)
        {
            System.out.println(i + ": " + nodeList.get(i).getBaseURL());
        }*/
    }
    
    /**
     * test the failed creation of a doc
     */
    //@Test
    public void testFailedCreate()
    {
        for(int i=0; i<nodeList.size(); i++)
        {
            currentUrl = nodeList.get(i).getBaseURL();
            d1 = new D1Client(currentUrl);
            
            try
            {
                printHeader("testFailedCreate - node " + nodeList.get(i).getBaseURL());
                checkTrue(1==1);
                String principal = "uid%3Dkepler,o%3Dunaffiliated,dc%3Decoinformatics,dc%3Dorg";
                AuthToken token = d1.login(principal, "kepler");
                String idString = prefix + ExampleUtilities.generateIdentifier();
                Identifier guid = new Identifier();
                guid.setValue(idString);
                InputStream objectStream = this.getClass().getResourceAsStream("/org/dataone/client/tests/knb-lter-luq.76.2-broken.xml");
                SystemMetadata sysmeta = generateSystemMetadata(guid, ObjectFormat.EML_2_1_0);
                Identifier rGuid = null;

                try {
                    rGuid = d1.create(token, guid, objectStream, sysmeta);
                    errorCollector.addError(new Throwable(createAssertMessage() + 
                            " Should have thrown exception since the xml file created was currupt"));
                } catch (Exception e) {
                }
            }
            catch(Exception e)
            {
                errorCollector.addError(new Throwable(createAssertMessage() + " unexpected error in testFailedCreate: " + e.getMessage()));
            }
        }
    }

    /**
     * test the getLogRecords call
     */
    //@Test
    public void testGetLogRecords()
    {
       for(int j=0; j<nodeList.size(); j++)
       {
           currentUrl = nodeList.get(j).getBaseURL();
           d1 = new D1Client(currentUrl);
           
           printHeader("testGetLogRecords - node " + nodeList.get(j).getBaseURL());
           System.out.println("current time is: " + new Date());
           try
           {
               Date start = new Date(System.currentTimeMillis() - 500000);
               String principal = "uid%3Dkepler,o%3Dunaffiliated,dc%3Decoinformatics,dc%3Dorg";
               AuthToken token = d1.login(principal, "kepler");

               String idString = prefix + ExampleUtilities.generateIdentifier();
               Identifier guid = new Identifier();
               guid.setValue(idString);
               InputStream objectStream = this.getClass().getResourceAsStream("/org/dataone/client/tests/knb-lter-cdr.329066.1.data");
               SystemMetadata sysmeta = generateSystemMetadata(guid, ObjectFormat.TEXT_CSV);

               Identifier rGuid = d1.create(token, guid, objectStream, sysmeta);
               InputStream data = d1.get(token, rGuid);
               String str = IOUtils.toString(data);
               //System.out.println("str: " + str);
               checkTrue(str.indexOf("61 66 104 2 103 900817 \"Planted\" 15.0  3.3") != -1);
               checkEquals(guid.getValue(), rGuid.getValue());

               //get the logs for the last minute
               Date end = new Date(System.currentTimeMillis() + 500000);
               System.out.println("start: " + start + " end: " + end);
               Log log = d1.getLogRecords(token, start, end, Event.CREATE);
               System.out.println("log size: " + log.sizeLogEntryList());
               boolean isfound = false;
               for(int i=0; i<log.sizeLogEntryList(); i++)
               { //check to see if our create event is in the log
                   LogEntry le = log.getLogEntry(i);
                   //System.out.println("le: " + le.getIdentifier().getValue());
                   //System.out.println("rGuid: " + rGuid.getValue());
                   if(le.getIdentifier().getValue().trim().equals(rGuid.getValue().trim()))
                   {
                       isfound = true;
                       System.out.println("log record found");
                       break;
                   }
               }
               System.out.println("isfound: " + isfound);
               checkTrue(isfound);

           } 
           catch(Exception e)
           {
               e.printStackTrace();
               errorCollector.addError(new Throwable(createAssertMessage() + " threw an unexpected exception: " + e.getMessage()));
           }
       }
    }
    
    /**
     * list objects with specified params
     */
    //@Test
    public void testListObjects()
    {
        for(int j=0; j<nodeList.size(); j++)
        {
            currentUrl = nodeList.get(j).getBaseURL();
            d1 = new D1Client(currentUrl);
            
            printHeader("testListObjects - node " + nodeList.get(j).getBaseURL());
            System.out.println("current time is: " + new Date());
            try
            {
                Date date1 = new Date(System.currentTimeMillis() - 1000000);
                String principal = "uid%3Dkepler,o%3Dunaffiliated,dc%3Decoinformatics,dc%3Dorg";
                AuthToken token = d1.login(principal, "kepler");
                //AuthToken token = new AuthToken("public");
                //create a document we know is in the system
                String idString = prefix + ExampleUtilities.generateIdentifier();
                Identifier guid = new Identifier();
                guid.setValue(idString);
                
                InputStream objectStream = this.getClass().getResourceAsStream("/org/dataone/client/tests/knb-lter-cdr.329066.1.data");
                SystemMetadata sysmeta = generateSystemMetadata(guid, ObjectFormat.TEXT_CSV);

                Identifier rGuid = d1.create(token, guid, objectStream, sysmeta);
                
                checkEquals(rGuid.getValue(), guid.getValue());
                
                //make the inserted documents public
                d1.setAccess(token, rGuid, "public", "read", "allow", "allowFirst");

                //get the objectList and make sure our created doc is in it
                ObjectList ol = d1.listObjects(token, null, null, null, false, 0, 1000);
                boolean isThere = false;
                
                checkTrue(ol.sizeObjectInfoList() > 0);
                
                //System.out.println("ol size: " + ol.sizeObjectInfoList());
                //System.out.println("guid: " + guid.getValue());
                for(int i=0; i<ol.sizeObjectInfoList(); i++)
                {
                    ObjectInfo oi = ol.getObjectInfo(i);
                    //System.out.println("oiid: " + oi.getIdentifier().getValue());
                    if(oi.getIdentifier().getValue().trim().equals(guid.getValue().trim()))
                    {
                        isThere = true;
                        //System.out.println("oi.checksum: " + oi.getChecksum().getValue() + 
                        //        "   sm.checksum: " + sysmeta.getChecksum().getValue());
                        break;
                    }
                }

                checkTrue(isThere);
                
                idString = prefix + ExampleUtilities.generateIdentifier();
                guid = new Identifier();
                guid.setValue(idString);
                objectStream = this.getClass().getResourceAsStream("/org/dataone/client/tests/knb-lter-cdr.329066.1.data");
                sysmeta = generateSystemMetadata(guid, ObjectFormat.TEXT_CSV);

                rGuid = d1.create(token, guid, objectStream, sysmeta);
                System.out.println("inserted doc with id " + rGuid.getValue());

                checkEquals(guid.getValue(), rGuid.getValue());
                
                //make the inserted documents public
                d1.setAccess(token, rGuid, "public", "read", "allow", "allowFirst");

                Date date2 = new Date(System.currentTimeMillis() + 1000000);

                ObjectList ol2 = d1.listObjects(token, date1, date2, null, false, 0, 1000);
                boolean isthere = false;
                for(int i=0; i<ol2.sizeObjectInfoList(); i++)
                {
                    ObjectInfo oi = ol2.getObjectInfo(i);
                    if(oi.getIdentifier().getValue().trim().equals(rGuid.getValue().trim()))
                    {
                        isthere = true;
                        break;
                    }
                }
                System.out.println("isthere: " + isthere);
                checkTrue(isthere);
                
                //test with a public token.  should get the same result since both docs are public
                token = new AuthToken("public");
                ol2 = d1.listObjects(token, null, null, null, false, 0, 1000);
                isthere = false;
                for(int i=0; i<ol2.sizeObjectInfoList(); i++)
                {
                    ObjectInfo oi = ol2.getObjectInfo(i);
                    if(oi.getIdentifier().getValue().trim().equals(rGuid.getValue().trim()))
                    {
                        isthere = true;
                        break;
                    }
                }
                System.out.println("isthere: " + isthere);
                checkTrue(isthere);
            }
            catch(Exception e)
            {
                e.printStackTrace();
                errorCollector.addError(new Throwable(createAssertMessage() + " could not list object: " + e.getMessage()));
            }
        }
    }

    /**
     * get a systemMetadata resource
     */
    //@Test
    public void testGetSystemMetadata()
    {
        for(int i=0; i<nodeList.size(); i++)
        {
            currentUrl = nodeList.get(i).getBaseURL();
            d1 = new D1Client(currentUrl);
            
            printHeader("testGetSystemMetadata - node " + nodeList.get(i).getBaseURL());
            try
            {
                //create a document
                String principal = "uid%3Dkepler,o%3Dunaffiliated,dc%3Decoinformatics,dc%3Dorg";
                AuthToken token = d1.login(principal, "kepler");
                String idString = prefix + ExampleUtilities.generateIdentifier();
                Identifier guid = new Identifier();
                guid.setValue(idString);
                InputStream objectStream = this.getClass().getResourceAsStream("/org/dataone/client/tests/knb-lter-cdr.329066.1.data");
                SystemMetadata sysmeta = generateSystemMetadata(guid, ObjectFormat.TEXT_CSV);
                Identifier rGuid = d1.create(token, guid, objectStream, sysmeta);
                checkEquals(guid.getValue(), rGuid.getValue());
                //System.out.println("create success, id returned is " + rGuid.getValue());

                //get the system metadata
                SystemMetadata sm = d1.getSystemMetadata(token, rGuid);
                checkTrue(guid.getValue().equals(sm.getIdentifier().getValue()));
            }
            catch(Exception e)
            {
                e.printStackTrace();
                errorCollector.addError(new Throwable(createAssertMessage() + " error in getSystemMetadata: " + e.getMessage()));
            }
        }
    }

    /**
     * test the update of a resource
     */
    //@Test
    public void testUpdate()
    {
        for(int i=0; i<nodeList.size(); i++)
        {
            currentUrl = nodeList.get(i).getBaseURL();
            d1 = new D1Client(currentUrl);
            
            printHeader("testUpdate - node " + nodeList.get(i).getBaseURL());
            try 
            {
                //create a document
                String principal = "uid%3Dkepler,o%3Dunaffiliated,dc%3Decoinformatics,dc%3Dorg";
                AuthToken token = d1.login(principal, "kepler");
                String idString = prefix + ExampleUtilities.generateIdentifier();
                Identifier guid = new Identifier();
                guid.setValue(idString);
                InputStream objectStream = this.getClass().getResourceAsStream("/org/dataone/client/tests/knb-lter-cdr.329066.1.data");
                SystemMetadata sysmeta = generateSystemMetadata(guid, ObjectFormat.TEXT_CSV);
                System.out.println("d1 create");
                Identifier rGuid = d1.create(token, guid, objectStream, sysmeta);
                System.out.println("d1 created " + rGuid.getValue());
                checkEquals(guid.getValue(), rGuid.getValue());
                //System.out.println("create success, id returned is " + rGuid.getValue());

                //get the document
                InputStream data = d1.get(token, rGuid);
                checkTrue(null != data);
                String str = IOUtils.toString(data);
                checkTrue(str.indexOf("61 66 104 2 103 900817 \"Planted\" 15.0  3.3") != -1);
                data.close();

                //alter the document
                Identifier newguid = new Identifier();
                newguid.setValue(prefix + ExampleUtilities.generateIdentifier());
                str = str.replaceAll("61", "0");
                objectStream = IOUtils.toInputStream(str);
                SystemMetadata updatedSysmeta = generateSystemMetadata(newguid, ObjectFormat.TEXT_CSV);

                //update the document
                System.out.println("d1 update newguid: "+ newguid.getValue() + " old guid: " + rGuid);
                Identifier nGuid = d1.update(token, newguid, objectStream, rGuid, updatedSysmeta);
                System.out.println("d1 updated success, id returned is " + nGuid.getValue());

                //perform tests
                data = d1.get(token, nGuid);
                checkTrue(null != data);
                str = IOUtils.toString(data);
                checkTrue(str.indexOf("0 66 104 2 103 900817 \"Planted\" 15.0  3.3") != -1);
                data.close();
            }
            catch(Exception e)
            {
                e.printStackTrace();
                errorCollector.addError(new Throwable(createAssertMessage() + " error in testUpdate: " + e.getMessage()));
            }
        }
    }

    /**
     * test the error state where metacat fails if the id includes a .\d on
     * the end.
     */
    //@Test
    public void testFailedCreateData() {
        for(int i=0; i<nodeList.size(); i++)
        {
            currentUrl = nodeList.get(i).getBaseURL();
            d1 = new D1Client(currentUrl);
            
            printHeader("testFailedCreateData - node " + nodeList.get(i).getBaseURL());
            /*try 
        {
            System.out.println();
            assertTrue(1==1);
            //AuthToken token = new AuthToken("public");
            String principal = "uid%3Dkepler,o%3Dunaffiliated,dc%3Decoinformatics,dc%3Dorg";
            AuthToken token = d1.login(principal, "kepler");

            InputStream objectStream = this.getClass().getResourceAsStream("/org/dataone/client/tests/BAYXXX_015ADCP015R00_20051215.50.9.xml");
            SystemMetadata sysmeta = getSystemMetadata("/org/dataone/client/tests/BAYXXX_015ADCP015R00_20051215.50.9_SYSMETA.xml");
            Identifier guid = sysmeta.getIdentifier();
            System.out.println("inserting with guid " + guid.getValue());
            Identifier rGuid = new Identifier();

            //insert
            rGuid = d1.create(token, guid, objectStream, sysmeta);
            assertEquals(guid.getValue(), rGuid.getValue());

            //get
            InputStream data = d1.get(token, rGuid);
            assertNotNull(data);
            String str = IOUtils.toString(data);
            System.out.println("output: " + str);
            assertTrue(str.indexOf("BAYXXX_015ADCP015R00_20051215.50.9") != -1);
            data.close();
        } 
        catch (Exception e) 
        {
            e.printStackTrace();
            fail("Error inserting: " + e.getMessage());
        }*/
            try
            {
                checkTrue(1==1);
                String principal = "uid%3Dkepler,o%3Dunaffiliated,dc%3Decoinformatics,dc%3Dorg";
                AuthToken token = d1.login(principal, "kepler");
                String idString = prefix + ExampleUtilities.generateIdentifier();
                Identifier guid = new Identifier();
                guid.setValue(idString + ".1.5.2");
                System.out.println("guid is " + guid.getValue());
                //InputStream objectStream = IOUtils.toInputStream("x,y,z\n1,2,3\n");
                InputStream objectStream = this.getClass().getResourceAsStream("/org/dataone/client/tests/BAYXXX_015ADCP015R00_20051215.50.9.xml");
                SystemMetadata sysmeta = generateSystemMetadata(guid, ObjectFormat.TEXT_CSV);
                Identifier rGuid = null;

                //insert
                rGuid = d1.create(token, guid, objectStream, sysmeta);
                checkEquals(guid.getValue(), rGuid.getValue());

                //get
                InputStream data = d1.get(token, rGuid);
                checkTrue(null != data);
                String str = IOUtils.toString(data);
                checkTrue(str.indexOf("BAYXXX_015ADCP015R00_20051215.50.9") != -1);
                data.close();
            }
            catch(Exception e)
            {
                errorCollector.addError(new Throwable(createAssertMessage() + " error in testFailedCreateData: " + e.getMessage()));
            }
        }
    }
    
    /**
     * test various create and get scenarios with different access rules
     */
    //@Test
    public void testGet() 
    {
        for(int i=0; i<nodeList.size(); i++)
        {
            currentUrl = nodeList.get(i).getBaseURL();
            d1 = new D1Client(currentUrl);
            
            printHeader("testGet - node " + nodeList.get(i).getBaseURL());
            try
            {
                //create a document
                String principal = "uid%3Dkepler,o%3Dunaffiliated,dc%3Decoinformatics,dc%3Dorg";
                AuthToken token = d1.login(principal, "kepler");
                String idString = prefix + ExampleUtilities.generateIdentifier();
                Identifier guid = new Identifier();
                guid.setValue(idString);
                InputStream objectStream = this.getClass().getResourceAsStream("/org/dataone/client/tests/knb-lter-luq.76.2.xml");
                //InputStream objectStream = IOUtils.toInputStream("<?xml version=\"1.0\"?><test></test>");
                SystemMetadata sysmeta = generateSystemMetadata(guid, ObjectFormat.EML_2_1_0);
                Identifier rGuid = null;
                rGuid = d1.create(token, guid, objectStream, sysmeta);
                checkEquals(guid.getValue(), rGuid.getValue());

                //try to get it as public.  this should fail
                AuthToken publicToken = new AuthToken("public");
                //this test is commented out because of this issue:
                //https://trac.dataone.org/ticket/706
                /*try
                {
                    InputStream data = d1.get(publicToken, rGuid);
                    System.out.println("data: " + IOUtils.toString(data));
                    fail("Should have thrown an exception.  Public can't get this doc yet.");
                }
                catch(Exception e)
                {

                }*/

                //change the perms, then try to get it again
                d1.setAccess(token, rGuid, "public", "read", "allow", "allowFirst");
                InputStream data = d1.get(publicToken, rGuid);
            }
            catch(Exception e)
            {
                e.printStackTrace();
                errorCollector.addError(new Throwable(createAssertMessage() + " error in testGet: " + e.getMessage()));
            }
        }
    }
    
    /**
     * test the creation of the desribes and describedBy sysmeta elements
     */
    @Test
    public void testCreateDescribedDataAndMetadata()
    {
        try
        {
            for(int j=0; j<nodeList.size(); j++)
            {
                currentUrl = nodeList.get(j).getBaseURL();
                d1 = new D1Client(currentUrl);
                String principal = "uid%3Dkepler,o%3Dunaffiliated,dc%3Decoinformatics,dc%3Dorg";
                //AuthToken token = d1.login(principal, "kepler");

                //create an EML document with a distribution element
                InputStream is = this.getClass().getResourceAsStream("/org/dataone/client/tests/eml200/dpennington.195.2");

                //parse that document for distribution info
                //Vector<String> distroUrls = getDistributionInfo(is);
                DataoneEMLParser parser = DataoneEMLParser.getInstance();
                EMLDocument emld = parser.parseDocument(is);
                checkEquals(ObjectFormat.EML_2_0_0.toString(), emld.format.toString());
                DistributionMetadata dm = emld.distributionMetadata.elementAt(0);
                checkEquals(ObjectFormat.TEXT_PLAIN.toString(), dm.mimeType);
                checkEquals(dm.url, "ecogrid://knb/IPCC.200802107062739.1");
                
                //create an ID for the metadata doc
                /*String idString = ExampleUtilities.generateIdentifier();
                Identifier mdId = new Identifier();
                mdId.setValue(idString);
                //create system metadata for the metadata doc
                //TODO: the object format should be set from the eml doc itself
                SystemMetadata mdSm = generateSystemMetadata(mdId, ObjectFormat.EML_2_0_0);

                //get the document(s) listed in the EML distribution elements
                //for the sake of this method, we're just going to get them from the resources directory
                //in an actual implementation, this would get the doc from the server
                for(int i=0; i<distroUrls.size(); i++)
                { 
                    String name = distroUrls.elementAt(i);
                    if(name.startsWith("ecogrid://knb"))
                    { //just handle ecogrid uris right now
                        name = name.substring(name.indexOf("ecogrid://knb/") + "ecogrid://knb/".length(), name.length());
                    }
                    else
                    {
                        System.out.println("Attempting to describe " + name + ", however ");
                        System.out.println("Describes/DescribesBy can only handle ecogrid:// urls at this time.");
                        continue;
                    }

                    //create Identifiers for each document
                    idString = ExampleUtilities.generateIdentifier();
                    Identifier id = new Identifier();
                    id.setValue(idString);
                    //create system metadata for the dist documents with a describedBy tag
                    //TODO: assume this is plain text, but really i should parse the EML 
                    //above to determine the mime type.
                    SystemMetadata sm = generateSystemMetadata(id, ObjectFormat.TEXT_PLAIN);
                    //add desrviedBy
                    sm.addDescribedBy(mdId);
                    //add describes to the metadata doc's sm
                    mdSm.addDescribe(id);
                    //TODO: replace this with a call to the server eventually
                    InputStream instream = this.getClass().getResourceAsStream("/org/dataone/client/tests/eml200/" + name);

                    Identifier createdDataId = d1.create(token, id, instream, sm);
                    d1.setAccess(token, createdDataId, "public", "read", "allow", "allowFirst");
                    checkEquals(createdDataId.getValue(), id.getValue());
                    System.out.println("Data ID: " + id.getValue());
                }
                
                //send the EML doc to create
                is = this.getClass().getResourceAsStream("/org/dataone/client/tests/dpennington.195.2.xml");
                Identifier createdMdId = d1.create(token, mdId, is, mdSm);
                d1.setAccess(token, createdMdId, "public", "read", "allow", "allowFirst");
                checkEquals(createdMdId.getValue(), mdId.getValue());
                System.out.println("Metadata ID: " + createdMdId.getValue());*/
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
            errorCollector.addError(new Throwable(createAssertMessage() + 
                    " error in testCreateDescribedDataAndMetadata: " + e.getMessage()));
        }
    }
    
    /**
     * test creation of data.  this also tests get() since it
     * is used to verify the inserted metadata
     */
    //@Test
    public void testCreateData() 
    {
        for(int i=0; i<nodeList.size(); i++)
        {
            currentUrl = nodeList.get(i).getBaseURL();
            d1 = new D1Client(currentUrl);

            printHeader("testCreateData - node " + nodeList.get(i).getBaseURL());
            try
            {
                checkTrue(1==1);
                String principal = "uid%3Dkepler,o%3Dunaffiliated,dc%3Decoinformatics,dc%3Dorg";
                AuthToken token = d1.login(principal, "kepler");
                String idString = prefix + ExampleUtilities.generateIdentifier();
                Identifier guid = new Identifier();
                guid.setValue(idString);
                InputStream objectStream = this.getClass().getResourceAsStream("/org/dataone/client/tests/knb-lter-cdr.329066.1.data");
                SystemMetadata sysmeta = generateSystemMetadata(guid, ObjectFormat.TEXT_CSV);
                Identifier rGuid = null;

                try {
                    rGuid = d1.create(token, guid, objectStream, sysmeta);
                    checkEquals(guid.getValue(), rGuid.getValue());
                } catch (Exception e) {
                    errorCollector.addError(new Throwable(createAssertMessage() + " error in testCreateData: " + e.getMessage()));
                }

                try {
                    InputStream data = d1.get(token, rGuid);
                    checkTrue(null != data);
                    String str = IOUtils.toString(data);
                    checkTrue(str.indexOf("61 66 104 2 103 900817 \"Planted\" 15.0  3.3") != -1);
                    data.close();
                } catch (Exception e) {
                    errorCollector.addError(new Throwable(createAssertMessage() + " error in testCreateData: " + e.getMessage()));
                } 
            }
            catch(Exception e)
            {
                errorCollector.addError(new Throwable(createAssertMessage() + " unexpected error in testCreateData: " + e.getMessage()));
            }
        }
    }
    
    /**
     * test creation of science metadata.  this also tests get() since it
     * is used to verify the inserted metadata
     */
    //@Test
    public void testCreateScienceMetadata() 
    {
        for(int i=0; i<nodeList.size(); i++)
        {
            currentUrl = nodeList.get(i).getBaseURL();
            d1 = new D1Client(currentUrl);
            
            try
            {
                printHeader("testCreateScienceMetadata - node " + nodeList.get(i).getBaseURL());
                checkTrue(1==1);
                String principal = "uid%3Dkepler,o%3Dunaffiliated,dc%3Decoinformatics,dc%3Dorg";
                AuthToken token = d1.login(principal, "kepler");
                String idString = prefix + ExampleUtilities.generateIdentifier();
                Identifier guid = new Identifier();
                guid.setValue(idString);
                InputStream objectStream = this.getClass().getResourceAsStream("/org/dataone/client/tests/knb-lter-luq.76.2.xml");
                SystemMetadata sysmeta = generateSystemMetadata(guid, ObjectFormat.EML_2_1_0);
                Identifier rGuid = null;

                try {
                    rGuid = d1.create(token, guid, objectStream, sysmeta);
                    checkEquals(guid.getValue(), rGuid.getValue());
                } catch (Exception e) {
                    errorCollector.addError(new Throwable(createAssertMessage() + " error in testCreateScienceMetadata: " + e.getMessage()));
                }


                try {
                    InputStream data = d1.get(token, rGuid);
                    checkTrue(null != data);
                    String str = IOUtils.toString(data);
                    checkTrue(str.indexOf("<shortName>LUQMetadata76</shortName>") != -1);
                    data.close();
                } catch (Exception e) {
                    errorCollector.addError(new Throwable(createAssertMessage() + " error in testCreateScienceMetadata: " + e.getMessage()));
                } 
            }
            catch(Exception e)
            {
                errorCollector.addError(new Throwable(createAssertMessage() + " unexpected error in testCreateScienceMetadata: " + e.getMessage()));
            }
        }
    }
    
    //@Test
    public void testDelete() 
    {
        for(int i=0; i<nodeList.size(); i++)
        {
            currentUrl = nodeList.get(i).getBaseURL();
            d1 = new D1Client(currentUrl);
            
            printHeader("testDelete - node " + nodeList.get(i).getBaseURL());
            checkTrue(1==1);
        }
    }
    
    //@Test
    public void testDescribe() 
    {
        for(int i=0; i<nodeList.size(); i++)
        {
            currentUrl = nodeList.get(i).getBaseURL();
            d1 = new D1Client(currentUrl);
            
            printHeader("testDescribe - node " + nodeList.get(i).getBaseURL());
            checkTrue(1==1);
        }
    }

    //@Test
    public void testGetNotFound() 
    {
        for(int i=0; i<nodeList.size(); i++)
        {
            currentUrl = nodeList.get(i).getBaseURL();
            d1 = new D1Client(currentUrl);
            
            try {
                printHeader("testGetNotFound - node " + nodeList.get(i).getBaseURL());
                String principal = "uid%3Dkepler,o%3Dunaffiliated,dc%3Decoinformatics,dc%3Dorg";
                AuthToken token = d1.login(principal, "kepler");
                Identifier guid = new Identifier();
                guid.setValue(bogusId);
                InputStream data = d1.get(token, guid);
                errorCollector.addError(new Throwable(createAssertMessage() + " NotFound exception should have been thrown"));
            }  catch (NotFound e) {
                String error = e.serialize(BaseException.FMT_XML);
                System.out.println(error);
                checkTrue(error.indexOf("<error") != -1);
            } catch (Exception e) {
                errorCollector.addError(new Throwable(createAssertMessage() + " unexpected exception in testGetNotFound: " + 
                        e.getMessage()));
            }
        }
    }
    
    //@Test
    public void testGetChecksumAuthTokenIdentifierType() 
    {
        checkTrue(1==1);
    }
    
    //@Test
    public void testGetChecksumAuthTokenIdentifierTypeString() 
    {
        checkTrue(1==1);
    }
    
    private static String createAssertMessage()
    {
        return "test failed at url " + currentUrl;
    }

    /** Generate a SystemMetadata object with bogus data. */
    private static SystemMetadata generateSystemMetadata(Identifier guid, ObjectFormat objectFormat) 
    {
        SystemMetadata sysmeta = new SystemMetadata();
        sysmeta.setIdentifier(guid);
        sysmeta.setObjectFormat(objectFormat);
        sysmeta.setSize(12);
        Principal submitter = new Principal();
        String dn = "uid=jones,o=NCEAS,dc=ecoinformatics,dc=org";
        submitter.setValue(dn);
        sysmeta.setSubmitter(submitter);
        Principal rightsHolder = new Principal();
        rightsHolder.setValue(dn);
        sysmeta.setRightsHolder(rightsHolder);
        sysmeta.setDateSysMetadataModified(new Date());
        sysmeta.setDateUploaded(new Date());
        NodeReference originMemberNode = new NodeReference();
        originMemberNode.setValue("mn1");
        sysmeta.setOriginMemberNode(originMemberNode);
        NodeReference authoritativeMemberNode = new NodeReference();
        authoritativeMemberNode.setValue("mn1");
        sysmeta.setAuthoritativeMemberNode(authoritativeMemberNode);
        Checksum checksum = new Checksum();
        checksum.setValue("4d6537f48d2967725bfcc7a9f0d5094ce4088e0975fcd3f1a361f15f46e49f83");
        checksum.setAlgorithm(ChecksumAlgorithm.SH_A256);
        sysmeta.setChecksum(checksum);
        return sysmeta;
    }

    /** Generate a science metadata object for testing. */
    private static String generateScienceMetadata(Identifier guid) {
        String accessBlock = ExampleUtilities.getAccessBlock("public", true, true,
                false, false, false);
        String emldoc = ExampleUtilities.generateEmlDocument(
                "Test identifier manager",
                ExampleUtilities.EML2_1_0, null,
                null, "http://fake.example.com/somedata", null,
                accessBlock, null, null, null, null);
        return emldoc;
    }
    
    /**
     * get system metadata
     * @param metadataResourcePath
     * @return
     */
    private SystemMetadata getSystemMetadata(String metadataResourcePath)  {
        printHeader("testGetSystemMetadata");
        SystemMetadata  systemMetadata = null;
        InputStream inputStream = null;
        try {
            IBindingFactory bfact =
                    BindingDirectory.getFactory(org.dataone.service.types.SystemMetadata.class);

            IMarshallingContext mctx = bfact.createMarshallingContext();
            IUnmarshallingContext uctx = bfact.createUnmarshallingContext();

            inputStream = this.getClass().getResourceAsStream(metadataResourcePath);

            systemMetadata = (SystemMetadata) uctx.unmarshalDocument(inputStream, null);

        } catch (JiBXException ex) {
            ex.printStackTrace();
            systemMetadata = null;
        } finally {
            try {
                inputStream.close();
            } catch (IOException ex) {
               ex.printStackTrace();
            }
        }
        return systemMetadata;
    }
    
    private void printHeader(String methodName)
    {
        System.out.println("\n***************** running test for " + methodName + " *****************");
    }
    
    private void checkEquals(final String s1, final String s2)
    {
        errorCollector.checkSucceeds(new Callable<Object>() 
        {
            public Object call() throws Exception 
            {
                assertThat("assertion failed for host " + currentUrl, s1, is(s2));
                //assertThat("assertion failed for host " + currentUrl, s1, is(s2 + "x"));
                return null;
            }
        });
    }
    
    private void checkTrue(final boolean b)
    {
        errorCollector.checkSucceeds(new Callable<Object>() 
        {
            public Object call() throws Exception 
            {
                assertThat("assertion failed for host " + currentUrl, true, is(b));
                return null;
            }
        });
    }
}
