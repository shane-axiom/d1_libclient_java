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
package org.dataone.service.types.v1.comparators;

import static org.junit.Assert.*;

import java.util.Date;

import org.dataone.service.types.v1.SystemMetadata;
import org.junit.Test;

/**
 * The Junit test class to test the class SysMetaModificationDateComparator.
 * @author tao
 *
 */
public class SysMetaModificationDateComparatorTest {
    @Test
    public void testCompares() {
        SystemMetadata meta1 = new SystemMetadata();
        Date date1 = new Date();
        date1.setTime(100000000);
        meta1.setDateSysMetadataModified(date1);
        
        SystemMetadata meta2 = new SystemMetadata();
        Date date2 = new Date();
        date2.setTime(200000000);
        meta2.setDateSysMetadataModified(date2);
        
        SysMetaModificationDateComparator comparator = new SysMetaModificationDateComparator();
        assertTrue("SysMetaModificationDateComparatorTest.testCompares - the meta1 should be less than meta2", comparator.compare(meta1, meta2) <0);
    }
}
