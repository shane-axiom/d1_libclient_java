/**
 *  '$RCSfile$'
 *  Copyright: 2008 Regents of the University of California and the
 *              National Center for Ecological Analysis and Synthesis
 *  Purpose: To test the Access Controls in metacat by JUnit
 *
 *   '$Author: jones $'
 *     '$Date: 2010-03-18 02:11:35 -0800 (Thu, 18 Mar 2010) $'
 * '$Revision: 5286 $'
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package org.dataone.client;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;
import java.util.TimeZone;
import java.util.Vector;

/**
 * Utilities that are useful for generating test data.
 */
public class TestUtilities {
	    
	protected final static String EML2_0_0 = "EML2_0_0";
	protected final static String EML2_0_1 = "EML2_0_1";
	protected final static String EML2_1_0 = "EML2_1_0";
	
	protected static final String ALLOWFIRST = "allowFirst";
	protected static final String DENYFIRST = "denyFirst";
			
	// header blocks
	protected final static String testEml_200_Header = "<?xml version=\"1.0\"?><eml:eml"
		+ " xmlns:eml=\"eml://ecoinformatics.org/eml-2.0.0\""
		+ " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
		+ " packageId=\"eml.1.1\" system=\"knb\""
		+ " xsi:schemaLocation=\"eml://ecoinformatics.org/eml-2.0.0 eml.xsd\""
		+ " scope=\"system\">";
	
	protected final static String testEml_201_Header = "<?xml version=\"1.0\"?><eml:eml"
		+ " xmlns:eml=\"eml://ecoinformatics.org/eml-2.0.1\""
		+ " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
		+ " packageId=\"eml.1.1\" system=\"knb\""
		+ " xsi:schemaLocation=\"eml://ecoinformatics.org/eml-2.0.1 eml.xsd\""
		+ " scope=\"system\">";
	
	protected final static String testEml_210_Header = "<?xml version=\"1.0\"?><eml:eml"
			+ " xmlns:eml=\"eml://ecoinformatics.org/eml-2.1.0\""
			+ " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
			+ " packageId=\"eml.1.1\" system=\"knb\""
			+ " xsi:schemaLocation=\"eml://ecoinformatics.org/eml-2.1.0 eml.xsd\""
			+ " scope=\"system\">";

	protected final static String testEmlCreatorBlock = "<creator scope=\"document\">                                       "
			+ " <individualName>                                                  "
			+ "    <surName>Smith</surName>                                       "
			+ " </individualName>                                                 "
			+ "</creator>                                                         ";

	protected final static String testEmlContactBlock = "<contact scope=\"document\">                                       "
			+ " <individualName>                                                  "
			+ "    <surName>Jackson</surName>                                     "
			+ " </individualName>                                                 "
			+ "</contact>                                                         ";

	protected final static String testEmlInlineBlock1 = "<inline>                                                           "
			+ "  <admin>                                                          "
			+ "    <contact>                                                      "
			+ "      <name>Operator</name>                                        "
			+ "      <institution>PSI</institution>                               "
			+ "    </contact>                                                     "
			+ "  </admin>                                                         "
			+ "</inline>                                                          ";

	protected final static String testEmlInlineBlock2 = "<inline>                                                           "
			+ "  <instrument>                                                     "
			+ "    <instName>LCQ</instName>                                       "
			+ "    <source type=\"ESI\"></source>                                 "
			+ "    <detector type=\"EM\"></detector>                              "
			+ "  </instrument>                                                    "
			+ "</inline>                                                          ";

	/*
	 * Returns an access block base on params passed and the default perm order -
	 * allow first
	 */
	protected static String getAccessBlock(String principal, boolean grantAccess, boolean read,
			boolean write, boolean changePermission, boolean all) {
		return getAccessBlock(principal, grantAccess, read, write, changePermission, all,
				ALLOWFIRST);
	}

	/**
	 * This function returns an access block based on the params passed
	 */
	protected static String getAccessBlock(String principal, boolean grantAccess, boolean read,
			boolean write, boolean changePermission, boolean all, String permOrder) {
		String accessBlock = "<access "
				+ "authSystem=\"ldap://ldap.ecoinformatics.org:389/dc=ecoinformatics,dc=org\""
				+ " order=\"" + permOrder + "\"" + " scope=\"document\"" + ">";

		accessBlock += generateOneAccessRule(principal, grantAccess, read, write,
				changePermission, all);
		accessBlock += "</access>";

		return accessBlock;

	}

	/*
	 * Gets eml access block base on given acccess rules and perm order
	 */
	protected static String getAccessBlock(Vector<String> accessRules, String permOrder) {
		String accessBlock = "<access "
				+ "authSystem=\"ldap://ldap.ecoinformatics.org:389/dc=ecoinformatics,dc=org\""
				+ " order=\"" + permOrder + "\"" + " scope=\"document\"" + ">";
		// adding rules
		if (accessRules != null && !accessRules.isEmpty()) {
			for (int i = 0; i < accessRules.size(); i++) {
				String rule = (String) accessRules.elementAt(i);
				accessBlock += rule;

			}
		}
		accessBlock += "</access>";
		return accessBlock;
	}

	/*
	 * Generates a access rule for given parameter. Note this xml portion
	 * doesn't include <access></access>
	 */
	protected static String generateOneAccessRule(String principal, boolean grantAccess,
			boolean read, boolean write, boolean changePermission, boolean all) {
		String accessBlock = "";

		if (grantAccess) {
			accessBlock = "<allow>";
		} else {
			accessBlock = "<deny>";
		}

		accessBlock = accessBlock + "<principal>" + principal + "</principal>";

		if (all) {
			accessBlock += "<permission>all</permission>";
		} else {
			if (read) {
				accessBlock += "<permission>read</permission>";
			}
			if (write) {
				accessBlock += "<permission>write</permission>";
			}
			if (changePermission) {
				accessBlock += "<permission>changePermission</permission>";
			}
		}

		if (grantAccess) {
			accessBlock += "</allow>";
		} else {
			accessBlock += "</deny>";
		}
		return accessBlock;

	}

	/**
	 * This function returns a valid eml document with no access rules 
	 */
	protected static String generateEmlDocument(String title, String emlVersion, String inlineData1,
			String inlineData2, String onlineUrl1, String onlineUrl2,
			String docAccessBlock, String inlineAccessBlock1, String inlineAccessBlock2,
			String onlineAccessBlock1, String onlineAccessBlock2) {

//		debug("getTestEmlDoc(): title=" + title + " inlineData1=" + inlineData1
//				+ " inlineData2=" + inlineData2 + " onlineUrl1=" + onlineUrl1
//				+ " onlineUrl2=" + onlineUrl2 + " docAccessBlock=" + docAccessBlock
//				+ " inlineAccessBlock1=" + inlineAccessBlock1 + " inlineAccessBlock2="
//				+ inlineAccessBlock2 + " onlineAccessBlock1=" + onlineAccessBlock1
//				+ " onlineAccessBlock2=" + onlineAccessBlock2);
		String testDocument = "";
		String header;
		if (emlVersion == EML2_0_0) {
			header = testEml_200_Header;
		} else if (emlVersion == EML2_0_1) {
			header = testEml_201_Header;
		} else {
			header = testEml_210_Header;
		}
		testDocument += header;
		
		// if this is a 2.1.0+ document, the document level access block sits
		// at the same level and before the dataset element.
		if (docAccessBlock != null && emlVersion.equals(EML2_1_0)) {
			testDocument += docAccessBlock;
		}
		
		testDocument += "<dataset scope=\"document\"><title>"
				+ title + "</title>" + testEmlCreatorBlock;

		if (inlineData1 != null) {
			testDocument = testDocument
					+ "<distribution scope=\"document\" id=\"inlineEntity1\">"
					+ inlineData1 + "</distribution>";
		}
		if (inlineData2 != null) {
			testDocument = testDocument
					+ "<distribution scope=\"document\" id=\"inlineEntity2\">"
					+ inlineData2 + "</distribution>";
		}
		if (onlineUrl1 != null) {
			testDocument = testDocument
					+ "<distribution scope=\"document\" id=\"onlineEntity1\">"
					+ "<online><url function=\"download\">" + onlineUrl1
					+ "</url></online></distribution>";
		}
		if (onlineUrl2 != null) {
			testDocument = testDocument
					+ "<distribution scope=\"document\" id=\"onlineEntity2\">"
					+ "<online><url function=\"download\">" + onlineUrl2
					+ "</url></online></distribution>";
		}
		testDocument += testEmlContactBlock;

		// if this is a 2.0.X document, the document level access block sits
		// inside the dataset element.
		if (docAccessBlock != null && 
				(emlVersion.equals(EML2_0_0) || emlVersion.equals(EML2_0_1))) {
			testDocument += docAccessBlock;
		}

		testDocument += "</dataset>";

		if (inlineAccessBlock1 != null) {
			testDocument += "<additionalMetadata>";
			testDocument += "<describes>inlineEntity1</describes>";
			testDocument += inlineAccessBlock1;
			testDocument += "</additionalMetadata>";
		}

		if (inlineAccessBlock2 != null) {
			testDocument += "<additionalMetadata>";
			testDocument += "<describes>inlineEntity2</describes>";
			testDocument += inlineAccessBlock2;
			testDocument += "</additionalMetadata>";
		}

		if (onlineAccessBlock1 != null) {
			testDocument += "<additionalMetadata>";
			testDocument += "<describes>onlineEntity1</describes>";
			testDocument += onlineAccessBlock1;
			testDocument += "</additionalMetadata>";
		}

		if (onlineAccessBlock2 != null) {
			testDocument += "<additionalMetadata>";
			testDocument += "<describes>onlineEntity2</describes>";
			testDocument += onlineAccessBlock2;
			testDocument += "</additionalMetadata>";
		}

		testDocument += "</eml:eml>";

		// System.out.println("Returning following document" + testDocument);
		return testDocument;
	}
	
	/**
	 * Create a unique identifier for testing insert and update.
	 * 
	 * @return a String identifier based on the current date and time
	 */
	protected static String generateIdentifier() {
		return TestUtilities.generateTimeString();
	}
	
    /** Generate a timestamp for use in IDs. */
    private static String generateTimeString()
    {
        StringBuffer guid = new StringBuffer();

        // Create a calendar to get the date formatted properly
        String[] ids = TimeZone.getAvailableIDs(-8 * 60 * 60 * 1000);
        SimpleTimeZone pdt = new SimpleTimeZone(-8 * 60 * 60 * 1000, ids[0]);
        pdt.setStartRule(Calendar.APRIL, 1, Calendar.SUNDAY, 2 * 60 * 60 * 1000);
        pdt.setEndRule(Calendar.OCTOBER, -1, Calendar.SUNDAY, 2 * 60 * 60 * 1000);
        Calendar calendar = new GregorianCalendar(pdt);
        Date trialTime = new Date();
        calendar.setTime(trialTime);
        guid.append(calendar.get(Calendar.YEAR));
        guid.append(calendar.get(Calendar.DAY_OF_YEAR));
        guid.append(calendar.get(Calendar.HOUR_OF_DAY));
        guid.append(calendar.get(Calendar.MINUTE));
        guid.append(calendar.get(Calendar.SECOND));
        guid.append(calendar.get(Calendar.MILLISECOND));

        return guid.toString();
    }
}
