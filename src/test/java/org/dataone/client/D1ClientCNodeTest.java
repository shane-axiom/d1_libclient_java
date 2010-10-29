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

import java.io.InputStream;
import java.util.concurrent.Callable;

import org.apache.commons.io.IOUtils;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.ObjectFormat;
import org.dataone.service.types.ObjectLocation;
import org.dataone.service.types.ObjectLocationList;
import org.dataone.service.types.SystemMetadata;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

/**
 * Test the DataONE Java client methods that focus on CN services.
 * @author Matthew Jones
 */
public class D1ClientCNodeTest  {

    private static final String cnUrl = "http://cn-dev.dataone.org/cn/";
    private static final String identifier = "resolve:testID:19395674819298";
    private static final String badIdentifier = "ThisIdentifierShouldNotExist";

        
    @Rule 
    public ErrorCollector errorCollector = new ErrorCollector();

//    @Before
    public void setUp() throws Exception 
    {
        
    }
    
    /**
     * test the resolve() operation on Coordinating Nodes
     */
    @Test
    public void testResolveNew() {
        try {
            D1Client d1 = new D1Client(cnUrl);
            CNode cn = d1.getCN();

            //insert a doc to resolv
            printHeader("testResolve - node " + cnUrl);
            //AuthToken token = new AuthToken();
            Identifier guid = new Identifier();
            guid.setValue(identifier);
            String currentUrl = "http://cn-dev.dataone.org/knb/";
            d1 = new D1Client(currentUrl);
            MNode mn = d1.getMN(currentUrl);
            String principal = "uid%3Dkepler,o%3Dunaffiliated,dc%3Decoinformatics,dc%3Dorg";
            AuthToken token = mn.login(principal, "kepler");
            String idString = "test" + ExampleUtilities.generateIdentifier();
            guid.setValue(idString);
            InputStream objectStream = this.getClass().getResourceAsStream("/org/dataone/client/tests/knb-lter-luq.76.2.xml");
            SystemMetadata sysmeta = (new D1ClientTest()).generateSystemMetadata(guid, ObjectFormat.EML_2_1_0);
            Identifier rGuid = null;
            try {
                rGuid = mn.create(token, guid, objectStream, sysmeta);
                mn.setAccess(token, rGuid, "public", "read", "allow", "allowFirst");
                checkEquals(guid.getValue(), rGuid.getValue());
            } catch (Exception e) {
                errorCollector.addError(new Throwable(createAssertMessage() + " error in testCreateScienceMetadata: " + e.getMessage()));
            }
        
            ObjectLocationList oll = cn.resolve(token, rGuid);

            for (ObjectLocation ol : oll.getObjectLocationList()) {
                System.out.println("Location: " + ol.getNodeIdentifier().getValue()
                        + " (" + ol.getUrl() + ")");
                checkTrue(ol.getUrl().contains(identifier));
            }
        } catch (BaseException e) {
            e.printStackTrace();
            errorCollector.addError(new Throwable(createAssertMessage()
                    + " error in testResolve: " + e.getMessage()));
        }
    }
        
    /**
     * test the resolve() operation on Coordinating Nodes
     * @throws InterruptedException 
     */
    @Test
    public void testResolve() throws InterruptedException {
        D1Client d1 = new D1Client(cnUrl);
        CNode cn = d1.getCN();
        
        printHeader("testResolve - node " + cnUrl);
        //AuthToken token = new AuthToken();
        AuthToken token = null;
        Identifier guid = new Identifier();
        guid.setValue(identifier);
        
        SystemMetadata sysmeta;
        try {
        	sysmeta = cn.getSystemMetadata(token, guid);
        } catch (NotFound e1) {
        	System.out.println(guid.getValue() + " not found on "+ cnUrl + 
        			" Creating it now to test resolve...");
        	
        	MNode mn = d1.getMN("http://cn-dev.dataone.org/knb/");
        	Identifier rGuid = null;
        	try {
        		String principal = "uid%3Dkepler,o%3Dunaffiliated,dc%3Decoinformatics,dc%3Dorg";
        		AuthToken tokenMN = mn.login(principal, "kepler");
//                String idString = prefix + ExampleUtilities.generateIdentifier();
//                Identifier guid = new Identifier();
//                guid.setValue(idString);
        		InputStream objectStream = this.getClass().getResourceAsStream("/org/dataone/client/tests/knb-lter-cdr.329066.1.data");
        		sysmeta = ExampleUtilities.generateSystemMetadata(guid, ObjectFormat.TEXT_CSV);
//        		Identifier rGuid = null;
        		rGuid = mn.create(tokenMN, guid, objectStream, sysmeta);
        		checkEquals(guid.getValue(), rGuid.getValue());
        		
        		System.out.println(guid.getValue() + " should be now created.");
        	
        	} catch (Exception e) {
        		errorCollector.addError(new Throwable(createAssertMessage() + " error in testResolve (mn.create): " + e.getMessage()));
            }
        	
        	try {
                InputStream data = mn.get(token, rGuid);
                checkTrue(null != data);
                String str = IOUtils.toString(data);
                checkTrue(str.indexOf("61 66 104 2 103 900817 \"Planted\" 15.0  3.3") != -1);
                data.close();
            } catch (Exception e) {
                errorCollector.addError(new Throwable(createAssertMessage() + " error in testCreateData: " + e.getMessage()));
            } 
        	
        	
        } catch (BaseException e) {
            e.printStackTrace();
            errorCollector.addError(new Throwable(createAssertMessage()
                    + " error in testResolve (getSystemMetadata): " + e.getMessage()));
        }
        
        // wait a couple seconds to let metacat index itself.  Probably unnecessary in most cases, but...
        Thread.sleep(2000);
        	
        try {
        	ObjectLocationList oll = cn.resolve(token, guid);

        	for (ObjectLocation ol : oll.getObjectLocationList()) {
        		System.out.println("Location: " + ol.getNodeIdentifier().getValue()
        				+ " (" + ol.getUrl() + ")");
        		checkTrue(ol.getUrl().contains(identifier));
        	}
        } catch (BaseException e) {
            e.printStackTrace();
            errorCollector.addError(new Throwable(createAssertMessage()
                    + " error in testResolve (resolve): " + e.getMessage()));
        }
    }
    
    /**
     * test the resolve() operation on Coordinating Nodes
     */
    @Test
    public void testInvalidResolve() {
        D1Client d1 = new D1Client(cnUrl);
        CNode cn = d1.getCN();

        printHeader("testResolve - node " + cnUrl);
        //AuthToken token = new AuthToken();
        AuthToken token = null;
        Identifier guid = new Identifier();
        guid.setValue(badIdentifier);
        try {
            ObjectLocationList oll = cn.resolve(token, guid);
            checkEquals("Should not reach this check, exception should have been generated.", "");
        } catch (BaseException e) {
            checkTrue(e instanceof NotFound);
        }
    }
    
    private static String createAssertMessage()
    {
        return "test failed at url " + cnUrl;
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
                assertThat("assertion failed for host " + cnUrl, s1, is(s2));
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
                assertThat("assertion failed for host " + cnUrl, true, is(b));
                return null;
            }
        });
    }
}
