/**
 * Copyright 2010 Regents of the University of California and the
 *                National Center for Ecological Analysis and Synthesis
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package org.dataone.client;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.Callable;

import org.junit.*;
import org.junit.rules.*;
import static org.junit.Assert.*;
import static  org.hamcrest.CoreMatchers.is;

import org.apache.commons.io.IOUtils;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidSystemMetadata;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.types.*;
import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IMarshallingContext;
import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;



/**
 * Test the DataONE Java client methods.
 * @author Matthew Jones
 */
public class D1ClientTest  {

    // TODO: move these hardcoded properties out to a test configuration
    //protected static String contextUrl = "http://localhost:8080/knb/";
    protected static String contextUrl = "http://knb-mn.ecoinformatics.org/knb/";
    //protected static String contextUrl = "http://mn-rpw/mn/";
    //protected static String contextUrl = "http://cn-dev.dataone.org/knb/";

    // TODO: use the create() and insert() methods to create predictable test data,
    // rather than hardcoding test assumptions here
    private static final String DOC_TEXT = "<surName>Smith</surName>";
    private static final String id = "knb:nceas:100:7";
    private static final String prefix = "knb:testid:";
    private static final String bogusId = "foobarbaz214";

    private D1Client d1 = null;
    private List<Node> nodeList = null;
    private boolean useNodeList = true;
    private static String currentUrl;
    
    private static String watchedLog;
    
    @Rule 
    public ErrorCollector errorCollector = new ErrorCollector();

    @Before
    public void setUp() throws Exception 
    {
        InputStream nodeRegStream = this.getClass().getResourceAsStream("/org/dataone/client/nodeRegistry.xml");
        NodeRegistry nr = null;
        try 
        {
            IBindingFactory bfact = BindingDirectory.getFactory(NodeRegistry.class);
            IUnmarshallingContext uctx = bfact.createUnmarshallingContext();
            nr = (NodeRegistry) uctx.unmarshalDocument(nodeRegStream, null);
        } 
        catch (JiBXException e) 
        {
            e.printStackTrace();
            throw new ServiceFailure("1190", "Failed to deserialize NodeRegistry: " + e.getMessage());
        }   
        
        if(nr != null)
        {
            nodeList = nr.getNodes();
            
        }
        
        if(nodeList == null || nodeList.size() == 0)
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
     * test the getLogRecords call
     */
    @Test
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
               InputStream objectStream = IOUtils.toInputStream("x,y,z\n1,2,3\n");
               SystemMetadata sysmeta = generateSystemMetadata(guid, ObjectFormat.TEXT_CSV);

               Identifier rGuid = d1.create(token, guid, objectStream, sysmeta);
               InputStream data = d1.get(token, rGuid);
               String str = IOUtils.toString(data);
               //System.out.println("str: " + str);
               checkTrue(str.indexOf("x,y,z") != -1);
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
                   if(le.getIdentifier().getValue().equals(rGuid.getValue()))
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
    @Test
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
                InputStream objectStream = IOUtils.toInputStream("x,y,z\n1,2,3\n");
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
                objectStream = IOUtils.toInputStream("x,y,z\n1,2,3\n");
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
                    if(oi.getIdentifier().getValue().equals(rGuid.getValue()))
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
                    if(oi.getIdentifier().getValue().equals(rGuid.getValue()))
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
    @Test
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
                InputStream objectStream = IOUtils.toInputStream("x,y,z\n1,2,3\n");
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
    @Test
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
                InputStream objectStream = IOUtils.toInputStream("x,y,z\n1,2,3\n");
                SystemMetadata sysmeta = generateSystemMetadata(guid, ObjectFormat.TEXT_CSV);
                Identifier rGuid = d1.create(token, guid, objectStream, sysmeta);
                checkEquals(guid.getValue(), rGuid.getValue());
                //System.out.println("create success, id returned is " + rGuid.getValue());

                //get the document
                InputStream data = d1.get(token, rGuid);
                checkTrue(null != data);
                String str = IOUtils.toString(data);
                checkTrue(str.indexOf("x,y,z\n1,2,3") != -1);
                data.close();

                //alter the document
                Identifier newguid = new Identifier();
                newguid.setValue(prefix + ExampleUtilities.generateIdentifier());
                str = str.replaceAll("x", "a");
                objectStream = IOUtils.toInputStream(str);
                SystemMetadata updatedSysmeta = generateSystemMetadata(newguid, ObjectFormat.TEXT_CSV);

                //update the document
                Identifier nGuid = d1.update(token, newguid, objectStream, rGuid, updatedSysmeta);
                //System.out.println("updated success, id returned is " + nGuid.getValue());

                //perform tests
                data = d1.get(token, nGuid);
                checkTrue(null != data);
                str = IOUtils.toString(data);
                checkTrue(str.indexOf("a,y,z\n1,2,3") != -1);
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
     * test creation of data.  this also tests get() since it
     * is used to verify the inserted metadata
     */
    @Test
    public void testCreateData() {
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
                InputStream objectStream = IOUtils.toInputStream("x,y,z\n1,2,3\n");
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
                    checkTrue(str.indexOf("x,y,z\n1,2,3") != -1);
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
     * test the error state where metacat fails if the id includes a .\d on
     * the end.
     */
    @Test
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
    @Test
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
     * test creation of science metadata.  this also tests get() since it
     * is used to verify the inserted metadata
     */
    @Test
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
    
    @Test
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
    
    @Test
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

    @Test
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
    
    @Test
    public void testGetChecksumAuthTokenIdentifierType() 
    {
        checkTrue(1==1);
    }
    
    @Test
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
