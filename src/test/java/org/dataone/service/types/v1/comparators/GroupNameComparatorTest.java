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

import org.dataone.service.types.v1.Group;
import org.dataone.service.types.v1.SystemMetadata;
import org.junit.Test;

/**
 * The Junit test class to test the class GroupNameComparator.
 * 
 * The Contract:
 * Compares its two arguments for order. Returns a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second.
 * In the foregoing description, the notation sgn(expression) designates the mathematical signum function, which is defined to return one of -1, 0, or 1 according to whether the value of expression is negative, zero or positive.
 *
 * 1. The implementor must ensure that sgn(compare(x, y)) == -sgn(compare(y, x)) for all x and y. (This implies that compare(x, y) must throw an exception if and only if compare(y, x) throws an exception.)
 *
 * 2. The implementor must also ensure that the relation is transitive: ((compare(x, y)>0) && (compare(y, z)>0)) implies compare(x, z)>0.
 *
 * 3. Finally, the implementor must ensure that compare(x, y)==0 implies that sgn(compare(x, z))==sgn(compare(y, z)) for all z.
 * 
 * It is generally the case, but not strictly required that (compare(x, y)==0) == (x.equals(y)). Generally speaking, any comparator that violates this condition should clearly indicate this fact. The recommended language is "Note: this comparator imposes orderings that are inconsistent with equals."
 *
 * 
 * 
 * @author tao
 *
 */
public class GroupNameComparatorTest {
 
	/*
	 * The implementor must ensure that sgn(compare(x, y)) == -sgn(compare(y, x)) for all x and y. 
	 * (This implies that compare(x, y) must throw an exception if and only if compare(y, x) throws an exception.)
	 */
	@Test
    public void testCompare_Contract_1() {
        Group group1 = new Group();
        group1.setGroupName("foo");
        
        Group group2 = new Group();
        group2.setGroupName("bar");
        
        Group group3 = new Group();
        group3.setGroupName("bar");
        
        
        GroupNameComparator comparator = new GroupNameComparator();
        assertTrue("SysMetaModificationDateComparatorTest.testCompares - the meta1 should be less than meta2", 
        		comparator.compare(group1, group2) > 0);
        
        assertTrue("SysMetaModificationDateComparatorTest.testCompares - the meta2 should be greater than meta1", 
        		comparator.compare(group2, group1) < 0);
        
        assertTrue("SysMetaModificationDateComparatorTest.testCompares - the meta2 should be equal to meta3", 
        		comparator.compare(group2, group3) == 0);
        
        
        String resultA;
        try {
        	comparator.compare(group1, null);
        	resultA = "int";
        } catch (Exception e) {
        	resultA = "exception";
        }
        
        String resultB;
        try {
        	comparator.compare(null, group1);
        	resultB = "int";
        } catch (Exception e) {
        	resultB = "exception";
        }
        
        assertTrue("GroupNameComparatorTest.testCompares - " +
        		"group1 vs. null should throw exception for both comparisons",
        		resultA.equals(resultB));
        
    }
	
	
	 /* The implementor must also ensure that the relation is transitive: 
	  * ((compare(x, y)>0) && (compare(y, z)>0)) implies compare(x, z)>0.
      */
	
	@Test
    public void testCompare_Contract_2() {
        Group group1 = new Group();
        group1.setGroupName("aaaaa");
        
        Group group2 = new Group();
        group2.setGroupName("bbbbb");
        
        Group group3 = new Group();
        group3.setGroupName("ccccc");
        
        GroupNameComparator comparator = new GroupNameComparator();
        assertTrue("SysMetaModificationDateComparatorTest.testCompares - is transitive", 
        		comparator.compare(group1, group2) < 0 && comparator.compare(group2, group3) < 0
        		&& comparator.compare(group1,group3) < 0);
        
 
    }

	/*
	 * Finally, the implementor must ensure that compare(x, y)==0 implies that 
	 * sgn(compare(x, z))==sgn(compare(y, z)) for all z.
	 */
	@Test
    public void testCompare_Contract_3() {
        Group group1 = new Group();
        group1.setGroupName("bbbbb");
        
        Group group2 = new Group();
        group2.setGroupName("bbbbb");
        
        Group group3 = new Group();
        group3.setGroupName("ccccc");
        
        Group group4 = new Group();
        group4.setGroupName("aaaaa");
        
        Group group5 = new Group();
        group5.setGroupName("bbbbb");
		
		SystemMetadata meta1 = new SystemMetadata();
        Date date1 = new Date();
        date1.setTime(200000000);
        meta1.setDateSysMetadataModified(date1);
        
        SystemMetadata meta2 = new SystemMetadata();
        Date date2 = new Date();
        date2.setTime(200000000);
        meta2.setDateSysMetadataModified(date2);
        
        SystemMetadata meta3 = new SystemMetadata();
        Date date3 = new Date();
        date3.setTime(300000000);
        meta3.setDateSysMetadataModified(date3);
        
        SystemMetadata meta4 = new SystemMetadata();
        Date date4 = new Date();
        date4.setTime(100000000);
        meta4.setDateSysMetadataModified(date4);
        
        SystemMetadata meta5 = new SystemMetadata();
        Date date5 = new Date();
        date5.setTime(100000000);
        meta5.setDateSysMetadataModified(date5);
        
        
        GroupNameComparator comparator = new GroupNameComparator();
        
        assertTrue("group1 and group2 should be equal", comparator.compare(group1, group2) == 0);
        
        assertTrue("sgn(compare(group1, group3)==sgn(compare(group2,group3))", 
        		comparator.compare(group1, group3) == comparator.compare(group2, group3));
        
        assertTrue("sgn(compare(group1, group4)==sgn(compare(group2,group4))", 
        		comparator.compare(group1, group4) == comparator.compare(group2, group4));
        
        assertTrue("sgn(compare(group1, group5)==sgn(compare(group2,group5))", 
        		comparator.compare(group1, group5) == comparator.compare(group2, group5));
        
    }
	
	
}


