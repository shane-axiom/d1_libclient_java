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

package org.dataone.client.datastore;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import org.dataone.service.types.v1.Identifier;
import org.junit.Before;
import org.junit.Test;

public class MemoryDataStoreTest {
    
    @Before
    public void setUp() throws Exception {
    }
    
    @Test
    public void testSetAndGet() throws Exception{
        MemoryDataStore store = new MemoryDataStore();
        String test = "test";
        InputStream input = new ByteArrayInputStream(test.getBytes());
        Identifier id = new Identifier();
        id.setValue("tao.123");
        store.set(id, input);
        InputStream fromStore = store.get(id);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] array = new byte[1024];
        int size = fromStore.read(array);
        while (size != -1) {
            output.write(array, 0, size);
            size = fromStore.read(array);
        }
        String gotString = output.toString();
        assertTrue("The string got from the data store should be \"test\".",test.equals(gotString));
        
        id.setValue("tao.2345");
        fromStore = store.get(id);
        assertTrue("The input stream object got from the data store should be null.",fromStore == null);
    }
    
}
