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

package org.dataone.configuration;

import static org.junit.Assert.assertEquals;

import org.junit.Test;


public class OverrideSettingsTest {
	
	/**
	 * Test lookup from original properties file in d1_common_java
	 */
	@Test
	public void testOriginal() {
		String foo = Settings.getConfiguration().getString("test.foo");
		assertEquals("default", foo);
	
	}
	
	/**
	 * Test looking up an overridden value in d1_libclient
	 * the resources on d1_libclient are loaded before the d1_common resources
	 * and therefore are considered to override them
	 */
	@Test
	public void testOverride() {
		String bar = Settings.getConfiguration().getString("test.bar");
		assertEquals("override", bar);
	
	}
}
