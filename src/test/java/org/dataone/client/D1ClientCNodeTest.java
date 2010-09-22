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

import java.util.concurrent.Callable;

import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.ObjectLocation;
import org.dataone.service.types.ObjectLocationList;
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
    private static final String identifier = "MD_ORNLDAAC_749_03032010095920";
    private static final String badIdentifier = "ThisIdentifierShouldNotExist";

        
    @Rule 
    public ErrorCollector errorCollector = new ErrorCollector();

    @Before
    public void setUp() throws Exception 
    {
        
    }
        
    /**
     * test the resolve() operation on Coordinating Nodes
     */
    @Test
    public void testResolve() {
        D1Client d1 = new D1Client(cnUrl);
        CNode cn = d1.getCN();

        printHeader("testResolve - node " + cnUrl);
        //AuthToken token = new AuthToken();
        AuthToken token = null;
        Identifier guid = new Identifier();
        guid.setValue(identifier);
        try {
            ObjectLocationList oll = cn.resolve(token, guid);
            for (ObjectLocation ol : oll.getLocations()) {
                System.out.println("Location: " + ol.getNode().getValue()
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
