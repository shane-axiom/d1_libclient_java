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

import java.util.Comparator;
import java.util.Date;

import org.dataone.service.types.v1.SystemMetadata;



/**
 * A Comparator that compares two SystemMetadata objects base on the 
 * DateSystemMetadataModified property (Date)
 * 
 * Note: this comparator imposes orderings that are inconsistent with equals.
 * @author tao
 *
 */
public class SysMetaModificationDateComparator implements Comparator<SystemMetadata> 
//This class does not implement Serializable as recommended because comparator orderings are inconsistent with equals

{
    /** 
     * Compares the order based on the modified date of the two SystemMetadata objects.
     * 
     * Note: this comparator imposes orderings that are inconsistent with equals.
     * @param sysmeta1
     * @param sysmeta2
     * @return a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second.
     * @throws ClassCastException 
     */
    @Override
    public int  compare(SystemMetadata sysmeta1, SystemMetadata sysmeta2) throws ClassCastException, NullPointerException {
      if(sysmeta1 == null || sysmeta2 == null) {
        throw new NullPointerException("SysMetaModifiedDateComparator.compare - the parameters of compare method can't be null.");
      }
      Date date1 = sysmeta1.getDateSysMetadataModified();
      Date date2 = sysmeta2.getDateSysMetadataModified();
      
      if(date1 == null || date2 == null) {
        throw new NullPointerException("meta1.getDateSysMetadataModified().compare - the modification date of the SystemMetadata can't be null.");
      }
      return date1.compareTo(date2);
    }

    /** 
     * Indicates whether some other object is "equal to" this comparator.
     * @param obj
     * @return true if the specified object is an instance of the SysMetaModifiedDateComparator; otherwise false.
     * @throws ClassCastException 
     */
    @Override
    public boolean equals(Object obj) {
      if(obj instanceof SysMetaModificationDateComparator ) {
        return true;
      } else {
        return false;
      }
    }
}
