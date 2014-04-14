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

import org.dataone.service.types.v1.Group;


/**
 * A comparator for comparing the Group objects base on the GroupName property (String)
 * 
 * Note: this comparator imposes orderings that are inconsistent with equals.
 * @author tao
 *
 */
public class GroupNameComparator implements Comparator<Group> 
//This class does not implement Serializable as recommended because comparator orderings are inconsistent with equals
{

  /** 
   * Compares order based on the String groupName of two Group objects.
   * 
   * Note: this comparator imposes orderings that are inconsistent with equals.
   * @param o1
   * @param o2
   * @return int 
   * @throws ClassCastException 
   */
  @Override
  public int  compare(Group group1, Group group2) throws ClassCastException, NullPointerException {
    if(group1 == null || group2 == null) {
      throw new NullPointerException("GroupNameComparator.compare - the parameters of compare method can't be null.");
    }

    if(group1.getGroupName() == null || group2.getGroupName() == null ) {
      throw new NullPointerException("GroupNameComparator.compare - the group names of the Group objects can't be null.");
    }
    return group1.getGroupName().compareTo(group2.getGroupName());
  }

  /** 
   * Indicates whether some other object is "equal to" this comparator.
   * @param obj
   * @return true if they are some class.
   * @throws ClassCastException 
   */
  @Override
  public boolean equals(Object obj) {
    if(obj instanceof GroupNameComparator ) {
      return true;
    } else {
      return false;
    }
  }
}
