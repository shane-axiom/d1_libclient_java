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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

/**
 * Unit tests for the DataONE Java client methods.
 * @author Matthew Jones
 */
public class D1ClientUnitTest  {

    @Rule 
    public ErrorCollector errorCollector = new ErrorCollector();

    @Before
    public void setUp() throws Exception 
    {
    }
    
    /**
     * test the failed creation of a doc
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
