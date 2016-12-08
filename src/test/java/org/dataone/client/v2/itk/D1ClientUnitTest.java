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

package org.dataone.client.v2.itk;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.dataone.client.v2.CNode;
import org.dataone.client.v2.itk.D1Client;
import org.dataone.client.v2.itk.D1Object;
import org.dataone.client.v1.types.D1TypeBuilder;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.SynchronizationFailed;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SubjectInfo;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

/**
 * Unit tests for the DataONE Java client methods.
 * @author Matthew Jones
 */
public class D1ClientUnitTest  {
    private static final String TEST_CN_URL = Settings.getConfiguration().getString("D1Client.CN_URL");

    @Rule 
    public ErrorCollector errorCollector = new ErrorCollector();

    @Before
    public void setUp() throws Exception 
    {
    }
    
    @Test
    public void getCnOrMnByUrlShouldWorkWhenCnBaseUrlEmpty() throws ServiceFailure, NotImplemented {
    	
//    	Settings.getConfiguration().setProperty("D1Client.CN_URL","");
    	D1Client.setCN("");
    	D1Client.getMN("http://someMN.org/mn");
    	System.out.println("got an MNode by url");
    	D1Client.getCN("http://someCN.org/cn");
    	System.out.println("got a CNode by url");
    	try {
    		D1Client.getCN();
    		fail("A null CN baseUrl should throw an exception when calling getCN()");
    		System.out.println("got a CN by default");
    	} 
    	catch (ServiceFailure e) {
    		; // expected
    	}
    	try {
    		D1Client.getMN(D1TypeBuilder.buildNodeReference("foo"));
    		fail("Should have thrown an exception when trying to getMN by NodeRef");
    	} catch (ServiceFailure e) {
			;// expected
		}  	
    }
    
    /**
     * test that trailing slashes do not affect the response of the node
     */
//    @Test
    public void testTrailingSlashes()
    {
    	// moved to d1_integration product because of dependency on knb instance 
    }

//    @Ignore("once we have a mock HttpClient client to test against, add this back in. Currently depends on external resource (cn-dev)")
//    @Test
//    public void testNodeMap() throws ServiceFailure, NotImplemented {
//        printHeader("testNodeMap");
//        CNode cn = D1Client.getCN();
//        String nodeId = cn.lookupNodeId(TEST_CN_URL);
//        assertTrue(nodeId != null);
//        System.out.println("Found nodeId = " + nodeId);
//        assertTrue(nodeId.contains("c3p0"));
//        String registeredUrl = cn.lookupNodeBaseUrl(nodeId);
//        assertTrue(registeredUrl != null);
//        System.out.println("Found nodeUrl = " + registeredUrl);
//        assertEquals(TEST_CN_URL, registeredUrl);
//    }
    
    @Ignore
    @Test
    public void testListSubjects() {
        printHeader("testListSubjects");
		try {
	        CNode cn = D1Client.getCN();
	        SubjectInfo subjects = null;
	        Session session = new Session();
	        session.setSubject(new Subject());
			subjects = cn.listSubjects(session, null, null, 0, 0);
	        assertTrue(subjects != null);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} 
    }
    
    //@Test
    public void testError() {
        printHeader("testError");
		try {
	        Session session = new Session();
	        session.setSubject(new Subject());
	        SynchronizationFailed message = new SynchronizationFailed("0000", "Test");
	        message.setPid("test_pid");
	        //message.setNodeId("test_node");
			D1Client.getMN("http://localhost:8080/knb/d1/mn").synchronizationFailed(session, message );
	        assertTrue(true);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} 
    }

    @Test
    public void testNullObjectCheck() {
        Identifier id = new Identifier();
        byte[] data = new byte[3];
        String info = "Test";
        List<Object> objects = Arrays.asList((Object)id, (Object)data, (Object)info);
        try {
            D1Object.checkNotNull(objects);
        } catch (InvalidRequest e) {
            fail("Object was incorrectly found to be null.");
        }

        Identifier nullId = null;
        objects = Arrays.asList((Object)nullId, (Object)data, (Object)info);
        try {
            D1Object.checkNotNull(objects);
            fail("Object nullId should have been found to be null.");
        } catch (InvalidRequest e) {
            // This is ok; object was null, and we should have caught an InvalidRequest
        }
    }
    
    /**
     * test the unit test harness
     */
    @Test
    public void testHarness()
    {
        printHeader("testHarness");
        assertTrue(true);
        assertEquals("1", "1");
    }
    
    private void printHeader(String methodName)
    {
        System.out.println("\n***************** running test for " + methodName + " *****************");
    }
    
}
