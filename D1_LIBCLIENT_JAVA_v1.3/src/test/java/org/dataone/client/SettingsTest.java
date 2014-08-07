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

import static org.junit.Assert.assertTrue;

import org.dataone.configuration.Settings;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the DataONE Java Settings class.
 * @author Matthew Jones
 */
public class SettingsTest  {

    @Before
    public void setUp() throws Exception 
    {
    }
    
    @Test
    public void testHarness()
    {
        assertTrue(true);
    }
    
    /**
     * test that at least one setting is properly loaded.
     */
    @Test
    public void testCNSettings()
    {
        printHeader("testCNSettings");
        System.out.println("CN_URL ==> " + Settings.getConfiguration().getString("D1Client.CN_URL"));
        assertTrue(Settings.getConfiguration().getString("D1Client.CN_URL").length() > 0);
    }
    
    private void printHeader(String methodName)
    {
        System.out.println("\n***************** running test for " + methodName + " *****************");
    }
}
