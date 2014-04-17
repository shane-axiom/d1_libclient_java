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

import org.dataone.service.types.v1.Person;

/**
 * A comparator for comparing the Person objects based on the FamilyName property (String).
 * 
 * Note: this comparator imposes orderings that are inconsistent with equals.
 * @author tao
 *
 */
public class PersonFamilyNameComparator implements Comparator<Person> 
//This class does not implement Serializable because comparator orderings are inconsistent with equals
{

  /** 
   * Compares order based on the String familyNames of two Person object.
   * 
   * Note: this comparator imposes orderings that are inconsistent with equals.
   * 
   * @param o1
   * @param o2
   * @return int 
   * @throws ClassCastException 
   */
  @Override
  public int  compare(Person person1, Person person2) 
  throws ClassCastException, NullPointerException 
  {
    if(person1 == null || person2 == null) {
      throw new NullPointerException("PersonFamilyNameComparator.compare - the parameters of compare method can't be null.");
    }

    if(person1.getFamilyName() == null || person2.getFamilyName() == null) {
      throw new NullPointerException("PersonFamilyNameComparator.compare - the family names of Person objects can't be null.");
    }
    return person1.getFamilyName().compareTo(person2.getFamilyName());
  }

  /** 
   * Indicates whether some other object is "equal to" this comparator.
   * @param obj
   * @return boolean 
   * @throws ClassCastException 
   */
  @Override
  public boolean equals(Object obj) {
    if(obj instanceof PersonFamilyNameComparator ) {
      return true;
    } else {
      return false;
    }
  }
}
