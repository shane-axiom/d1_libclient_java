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
 * 
 * $Id$
 */

package org.dataone.client;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

/**
 * Test to make sure this package can access the common resources found in
 * the d1_test_resources package.  
 * 
 * @author rnahf
 */
public class CommonResourcesTest {

	@Test
	public final void testTest()
	{
		assertTrue("one".equals("one"));
	}
	/*
	 * this should work even without explicit dependency on d1_test_resources
	 * as it is inherited via d1_common_java dependency
	 */
	@Test
	public final void testCommonResourcesAvailability() throws IOException
	{
		String resource = "/D1shared/selfTest/simpleDummyResource.txt";
		InputStream is = this.getClass().getResourceAsStream(resource);
		if (is == null)
			fail("could not find resource in d1_test_resouces package: " + resource);
	}
}
