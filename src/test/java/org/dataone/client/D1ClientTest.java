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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

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
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Checksum;
import org.dataone.service.types.ChecksumAlgorithm;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.NodeReference;
import org.dataone.service.types.ObjectFormat;
import org.dataone.service.types.Principal;
import org.dataone.service.types.SystemMetadata;

/**
 * Test the DataONE Java client methods.
 * @author Matthew Jones
 */
public class D1ClientTest extends TestCase {

    // TODO: move these hardcoded properties out to a test configuration
    protected static String contextUrl = "http://localhost:8080/knb/";  
    
    // TODO: use the create() and insert() methods to create predicatable test data,
    // rather than hardcoding test assumptions here
    private static final String DOC_TEXT = "Biomass and growth of 20-year-old stands of Scots pine datasets";   
    private static final String id = "knb:nceas:100:7";
    private static final String prefix = "knb:testid:";
    private static final String bogusId = "foobarbaz214";
    
    private D1Client d1 = null;
    
    protected void setUp() throws Exception {
        super.setUp();
        d1 = new D1Client(contextUrl);
    }

    public void testAFileWrite() {
        try {
            File newFile = new File("/tmp/somefilelarge");
            OutputStream out = new FileOutputStream(newFile);
            String fakedata = "This is fake data.\n";
            InputStream fd = IOUtils.toInputStream(fakedata);
            long len = IOUtils.copyLarge(fd, out);
            out.flush();
            fd.close();
            out.close();
            assertTrue(len > 0);
        } catch (IOException ioe) {
            fail("File was not written:" + ioe.getMessage());
        }
    }

    public void testCreate() {
        assertTrue(1==1);
        AuthToken token = new AuthToken("public");
        String idString = prefix + generateTimeString();
        Identifier guid = new Identifier();
        guid.setValue(idString);
        InputStream objectStream = IOUtils.toInputStream("x,y,z\n1,2,3\n");
        SystemMetadata sysmeta = generateSystemMetadata(guid);

        try {
            Identifier rGuid = d1.create(token, guid, objectStream, sysmeta);
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
    }

    public void testDelete() {
        assertTrue(1==1);
    }

    public void testDescribe() {
        assertTrue(1==1);
    }

    public void testGet() {
        try {
            AuthToken token = new AuthToken("public");
            Identifier guid = new Identifier();
            guid.setValue(id);
            InputStream data = d1.get(token, guid);
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

    public void testGetSystemMetadata() {
        assertTrue(1==1);
    }

    public void testUpdate() {
        assertTrue(1==1);
    }

    /** Generate a timestamp for use in IDs. */
    private String generateTimeString()
    {
        StringBuffer guid = new StringBuffer();

        // Create a calendar to get the date formatted properly
        String[] ids = TimeZone.getAvailableIDs(-8 * 60 * 60 * 1000);
        SimpleTimeZone pdt = new SimpleTimeZone(-8 * 60 * 60 * 1000, ids[0]);
        pdt.setStartRule(Calendar.APRIL, 1, Calendar.SUNDAY, 2 * 60 * 60 * 1000);
        pdt.setEndRule(Calendar.OCTOBER, -1, Calendar.SUNDAY, 2 * 60 * 60 * 1000);
        Calendar calendar = new GregorianCalendar(pdt);
        Date trialTime = new Date();
        calendar.setTime(trialTime);
        guid.append(calendar.get(Calendar.YEAR));
        guid.append(calendar.get(Calendar.DAY_OF_YEAR));
        guid.append(calendar.get(Calendar.HOUR_OF_DAY));
        guid.append(calendar.get(Calendar.MINUTE));
        guid.append(calendar.get(Calendar.SECOND));
        guid.append(calendar.get(Calendar.MILLISECOND));

        return guid.toString();
    }

    /** Generate a SystemMetadata object with bogus data. */
    private SystemMetadata generateSystemMetadata(Identifier guid) {
        SystemMetadata sysmeta = new SystemMetadata();
        sysmeta.setIdentifier(guid);
        sysmeta.setObjectFormat(ObjectFormat.TEXT_CSV);
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
}
