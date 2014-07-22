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
package org.dataone.client.formats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.dataone.client.v2.formats.ObjectFormatInfo;
import org.dataone.service.types.v1.ObjectFormat;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.junit.Test;

/**
 * Unit tests for the ObjectFormatInfo class.
 */
public class ObjectFormatInfoTest {

    @Test
    public void testInstance() {
        ObjectFormatInfo ofi = ObjectFormatInfo.instance();
        if (ofi == null) {
            fail("ObjectFormatInfo instance was null.");
        }
    }

    @Test
    public void testGetMimeTypeString() {
        String formatID = "text/csv";
        String correctMimeType = "text/csv";
        ObjectFormatInfo ofi = ObjectFormatInfo.instance();
        String result = ofi.getMimeType(formatID);
        assertEquals(result, correctMimeType);
    }

    @Test
    public void testGetMimeTypeObjectFormat() {
        String formatID = "text/csv";
        String correctMimeType = "text/csv";
        ObjectFormatInfo ofi = ObjectFormatInfo.instance();
        ObjectFormatIdentifier ofFormatID = new ObjectFormatIdentifier();
        ofFormatID.setValue(formatID);
        ObjectFormat of = new ObjectFormat();
        of.setFormatId(ofFormatID);
        String result = ofi.getMimeType(of);
        assertEquals(result, correctMimeType);
    }

    @Test
    public void testGetExtensionString() {
        String formatID = "text/csv";
        String correctExt = ".csv";
        ObjectFormatInfo ofi = ObjectFormatInfo.instance();
        String result = ofi.getExtension(formatID);
        assertEquals(result, correctExt);
    }

    @Test
    public void testGetExtensionObjectFormat() {
        String formatID = "text/csv";
        String correctExt = ".csv";
        ObjectFormatInfo ofi = ObjectFormatInfo.instance();
        ObjectFormatIdentifier ofFormatID = new ObjectFormatIdentifier();
        ofFormatID.setValue(formatID);
        ObjectFormat of = new ObjectFormat();
        of.setFormatId(ofFormatID);
        String result = ofi.getExtension(of);
        assertEquals(result, correctExt);
    }
}
