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

import junit.framework.TestCase;

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



/**
 * Test the DataONE Java client methods.
 * @author Matthew Jones
 */
public class D1ClientTest extends TestCase {

    // TODO: move these hardcoded properties out to a test configuration
    protected static String contextUrl = "http://localhost:8080/knb/";  
    
    // TODO: use the create() and insert() methods to create predictable test data,
    // rather than hardcoding test assumptions here
    private static final String DOC_TEXT = "<surName>Smith</surName>";   
    private static final String id = "knb:nceas:100:7";
    private static final String prefix = "knb:testid:";
    private static final String bogusId = "foobarbaz214";
    
    private D1Client d1 = null;
    
    protected void setUp() throws Exception {
        super.setUp();
        d1 = new D1Client(contextUrl);
    }
    
    /**
     * list objects with specified params
     */
    public void testListObjects()
    {
        try
        {
            System.out.println("Testing listObjects");
            String principal = "uid%3Dkepler,o%3Dunaffiliated,dc%3Decoinformatics,dc%3Dorg";
            AuthToken token = d1.login(principal, "kepler");
            //AuthToken token = new AuthToken("public");
            //create a document we know is in the system
            String idString = prefix + TestUtilities.generateIdentifier();
            Identifier guid = new Identifier();
            guid.setValue(idString);
            InputStream objectStream = IOUtils.toInputStream("x,y,z\n1,2,3\n");
            SystemMetadata sysmeta = generateSystemMetadata(guid, ObjectFormat.TEXT_CSV);
            Identifier rGuid = d1.create(token, guid, objectStream, sysmeta);
            System.out.println("doc inserted with guid: " + rGuid.getValue());
            assertEquals(guid.getValue(), rGuid.getValue());
            //make the inserted documents public
            d1.setAccess(token, rGuid, "public", "read", "allow", "allowFirst");
            
            //get the objectList and make sure our created doc is in it
            ObjectList ol = d1.listObjects(token, null, null, null, false, 0, 1000);
            boolean isThere = false;
            assertTrue(ol.sizeObjectInfoList() > 0);
            //System.out.println("ol size: " + ol.sizeObjectInfoList());
            //System.out.println("guid: " + guid.getValue());
            for(int i=0; i<ol.sizeObjectInfoList(); i++)
            {
                ObjectInfo oi = ol.getObjectInfo(i);
                //System.out.println("oiid: " + oi.getIdentifier().getValue());
                if(oi.getIdentifier().getValue().trim().equals(guid.getValue().trim()))
                {
                    isThere = true;
                    assertTrue(oi.getChecksum().getValue().equals(sysmeta.getChecksum().getValue()));
                    break;
                }
            }
            
            assertTrue(isThere);
        }
        catch(Exception e)
        {
            fail("Could not list objects: " + e.getMessage());
        }
    }
    
    /**
     * get a systemMetadata resource
     */
    public void testGetSystemMetadata()
    {
        try
        {
            //create a document
            AuthToken token = new AuthToken("public");
            String idString = prefix + TestUtilities.generateIdentifier();
            Identifier guid = new Identifier();
            guid.setValue(idString);
            InputStream objectStream = IOUtils.toInputStream("x,y,z\n1,2,3\n");
            SystemMetadata sysmeta = generateSystemMetadata(guid, ObjectFormat.TEXT_CSV);
            Identifier rGuid = d1.create(token, guid, objectStream, sysmeta);
            assertEquals(guid.getValue(), rGuid.getValue());
            System.out.println("create success, id returned is " + rGuid.getValue());
            
            //get the system metadata
            SystemMetadata sm = d1.getSystemMetadata(token, rGuid);
            assertTrue(guid.getValue().equals(sm.getIdentifier().getValue()));
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("Error in getSystemMetadata: " + e.getMessage());
        }
    }
    
    /**
     * test the update of a resource
     */
    public void testUpdate() 
    {
        try 
        {
            //create a document
            AuthToken token = new AuthToken("public");
            String idString = prefix + TestUtilities.generateIdentifier();
            Identifier guid = new Identifier();
            guid.setValue(idString);
            InputStream objectStream = IOUtils.toInputStream("x,y,z\n1,2,3\n");
            SystemMetadata sysmeta = generateSystemMetadata(guid, ObjectFormat.TEXT_CSV);
            Identifier rGuid = d1.create(token, guid, objectStream, sysmeta);
            assertEquals(guid.getValue(), rGuid.getValue());
            System.out.println("create success, id returned is " + rGuid.getValue());
            
            //get the document
            InputStream data = d1.get(token, rGuid);
            assertNotNull(data);
            String str = IOUtils.toString(data);
            assertTrue(str.indexOf("x,y,z\n1,2,3\n") != -1);
            data.close();
            
            //alter the document
            Identifier newguid = new Identifier();
            newguid.setValue(prefix + TestUtilities.generateIdentifier());
            str = str.replaceAll("x", "a");
            objectStream = IOUtils.toInputStream(str);
            SystemMetadata updatedSysmeta = generateSystemMetadata(newguid, ObjectFormat.TEXT_CSV);
            
            //update the document
            Identifier nGuid = d1.update(token, newguid, objectStream, rGuid, updatedSysmeta);
            System.out.println("updated success, id returned is " + nGuid.getValue());
                
            //perform tests
            data = d1.get(token, nGuid);
            assertNotNull(data);
            str = IOUtils.toString(data);
            assertTrue(str.indexOf("a,y,z\n1,2,3\n") != -1);
            data.close();
            
        } 
        catch(Exception e)
        {
            e.printStackTrace();
            fail("Error in testUpdate: " + e.getMessage());
        }
    }

    /**
     * test creation of data.  this also tests get() since it
     * is used to verify the inserted metadata
     */
    public void testCreateData() {
        assertTrue(1==1);
        AuthToken token = new AuthToken("public");
        String idString = prefix + TestUtilities.generateIdentifier();
        Identifier guid = new Identifier();
        guid.setValue(idString);
        InputStream objectStream = IOUtils.toInputStream("x,y,z\n1,2,3\n");
        SystemMetadata sysmeta = generateSystemMetadata(guid, ObjectFormat.TEXT_CSV);
        Identifier rGuid = null;

        try {
            rGuid = d1.create(token, guid, objectStream, sysmeta);
            assertEquals(guid.getValue(), rGuid.getValue());
        } catch (InvalidToken e) {
            fail(e.getMessage());
        } catch (ServiceFailure e) {
            e.printStackTrace();
            fail(e.getMessage());
        } catch (NotAuthorized e) {
            fail(e.getMessage());
        } catch (IdentifierNotUnique e) {
            fail(e.getMessage());
        } catch (UnsupportedType e) {
            fail(e.getMessage());
        } catch (InsufficientResources e) {
            fail(e.getMessage());
        } catch (InvalidSystemMetadata e) {
            fail(e.getMessage());
        } catch (NotImplemented e) {
            fail(e.getMessage());
        }
        
        try {
            InputStream data = d1.get(token, rGuid);
            assertNotNull(data);
            String str = IOUtils.toString(data);
            assertTrue(str.indexOf("x,y,z\n1,2,3\n") != -1);
            data.close();
        } catch (InvalidToken e) {
            fail(e.getDescription());
        } catch (ServiceFailure e) {
            fail(e.getDescription());
        } catch (NotAuthorized e) {
            fail(e.getDescription());
        } catch (NotFound e) {
            fail(e.getDescription());
        } catch (NotImplemented e) {
            fail(e.getDescription());
        } catch (IOException e) {
            fail("get() test failed while closing data stream. " + e.getMessage());
        }
    }

    /**
     * test creation of science metadata.  this also tests get() since it
     * is used to verify the inserted metadata
     */
    public void testCreateScienceMetadata() {
        assertTrue(1==1);
        AuthToken token = new AuthToken("public");
        String idString = prefix + TestUtilities.generateIdentifier();
        Identifier guid = new Identifier();
        guid.setValue(idString);
        String scimeta = generateScienceMetadata(guid);
        InputStream objectStream = IOUtils.toInputStream(scimeta);
        SystemMetadata sysmeta = generateSystemMetadata(guid, ObjectFormat.EML_2_1_0);
        Identifier rGuid = null;
        
        try {
            rGuid = d1.create(token, guid, objectStream, sysmeta);
            assertEquals(guid.getValue(), rGuid.getValue());
        } catch (InvalidToken e) {
            fail(e.getMessage());
        } catch (ServiceFailure e) {
            fail(e.getMessage());
        } catch (NotAuthorized e) {
            fail(e.getMessage());
        } catch (IdentifierNotUnique e) {
            fail(e.getMessage());
        } catch (UnsupportedType e) {
            fail(e.getMessage());
        } catch (InsufficientResources e) {
            fail(e.getMessage());
        } catch (InvalidSystemMetadata e) {
            fail(e.getMessage());
        } catch (NotImplemented e) {
            fail(e.getMessage());
        }
        
        
        try {
            InputStream data = d1.get(token, rGuid);
            assertNotNull(data);
            String str = IOUtils.toString(data);
            assertTrue(str.indexOf(DOC_TEXT) != -1);
            data.close();
        } catch (InvalidToken e) {
            fail(e.getDescription());
        } catch (ServiceFailure e) {
            fail(e.getDescription());
        } catch (NotAuthorized e) {
            fail(e.getDescription());
        } catch (NotFound e) {
            fail(e.getDescription());
        } catch (NotImplemented e) {
            fail(e.getDescription());
        } catch (IOException e) {
            fail("get() test failed while closing data stream. " + e.getMessage());
        }
    }
    
    public void testDelete() {
        assertTrue(1==1);
    }

    public void testDescribe() {
        assertTrue(1==1);
    }
    
    public void testGetNotFound() {
        try {
            AuthToken token = new AuthToken("public");
            Identifier guid = new Identifier();
            guid.setValue(bogusId);
            InputStream data = d1.get(token, guid);
            fail("NotFound exception should have been thrown for non-existent ID.");
        } catch (InvalidToken e) {
            fail(e.getDescription());
        } catch (ServiceFailure e) {
            fail(e.getDescription());
        } catch (NotAuthorized e) {
            fail(e.getDescription());
        } catch (NotFound e) {
            String error = e.serialize(BaseException.FMT_XML);
            System.out.println(error);
            assertTrue(error.indexOf("<error") != -1);
        } catch (NotImplemented e) {
            fail(e.getDescription());
        }
    }

    public void testGetChecksumAuthTokenIdentifierType() {
        assertTrue(1==1);
    }

    public void testGetChecksumAuthTokenIdentifierTypeString() {
        assertTrue(1==1);
    }

    public void testGetLogRecords() {
        assertTrue(1==1);
    }

    /** Generate a SystemMetadata object with bogus data. */
    private static SystemMetadata generateSystemMetadata(Identifier guid, ObjectFormat objectFormat) {
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
        String accessBlock = TestUtilities.getAccessBlock("public", true, true,
                false, false, false);
        String emldoc = TestUtilities.generateEmlDocument(
                "Test identifier manager", 
                TestUtilities.EML2_1_0, null,
                null, "http://fake.example.com/somedata", null,
                accessBlock, null, null, null, null);
        return emldoc;
    }
}
