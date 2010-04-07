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

import java.io.IOException;
import java.io.InputStream;

import junit.framework.TestCase;

import org.apache.commons.io.IOUtils;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.IdentifierType;

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
    private static final String bogusId = "foobarbaz214";
    
    private D1Client d1 = null;
    
    protected void setUp() throws Exception {
        super.setUp();
        d1 = new D1Client(contextUrl);
    }

    public void testCreate() {
        assertTrue(1==1);
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
            IdentifierType guid = new IdentifierType(id);
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
            IdentifierType guid = new IdentifierType(bogusId);
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
}
