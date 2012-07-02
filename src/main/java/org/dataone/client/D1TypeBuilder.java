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

import org.dataone.service.types.v1.AccessRule;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Person;
import org.dataone.service.types.v1.Subject;

public class D1TypeBuilder {


	public static NodeReference buildNodeReference(String value) {
		NodeReference id = new NodeReference();
		id.setValue(value);
		return id;
	}
	
	
	public static ObjectFormatIdentifier buildFormatIdentifier(String value) {
		ObjectFormatIdentifier fid = new ObjectFormatIdentifier();
		fid.setValue(value);
		return fid;
	}

	
	public static Identifier buildIdentifier(String value) {
		Identifier id = new Identifier();
		id.setValue(value);
		return id;
	}
	
	/**
	 * Validates the identifier checking for any invalid characters
	 * The only rule currently is no whitespace.
	 * @param identifier
	 * @return true if the identifier is valid
	 */
	public static boolean isIdentifierValid(Identifier identifier)
	{
		
		String whiteSpaceRegex = "\\s";
		if (identifier.getValue().matches(whiteSpaceRegex)) {
			return false;
		}
		
		return true;
	}
	
	
	public static Subject buildSubject(String value) 
	{
		Subject s = new Subject();
		s.setValue(value);
		return s;
	}


	public static AccessRule buildAccessRule(String subjectString, Permission permission)
	{
		if (subjectString == null || permission == null) {
			return null;
		}
		AccessRule ar = new AccessRule();
		ar.addSubject(buildSubject(subjectString));
		ar.addPermission(permission);
		return ar;
	}
	
	public static AccessRule buildAccessRule(String subjectString, Permission[] permissions)
	{
		if (subjectString == null || permissions == null) {
			return null;
		}
		AccessRule ar = new AccessRule();
		ar.addSubject(buildSubject(subjectString));

		for (Permission p: permissions)
			ar.addPermission(p);

		return ar;
	}


	public static Person buildPerson(Subject subject, String familyName, 
			String givenName, String emailString) 
	{
		String[] badParam = new String[]{};
		Person person = new Person();
		//	  try {
			//		InternetAddress ia = new InternetAddress(emailString, true);
		if (emailString == null || emailString.trim().equals(""))
			badParam[badParam.length] = "emailString";
		if (familyName == null || familyName.trim().equals(""))
			badParam[badParam.length] = "familyName";
		if (givenName == null || givenName.trim().equals(""))
			badParam[badParam.length] = "givenName";
		if (subject == null || subject.getValue().equals(""))
			badParam[badParam.length] = "subject";

		if (badParam.length > 0)
			throw new IllegalArgumentException("null or empty string values for parameters: " + badParam);

		//	} catch (AddressException e) {
		//		// thrown by InternetAddress constructor
		//	}

		person.addEmail(emailString);
		person.addGivenName(givenName);
		person.setFamilyName(familyName);
		person.setSubject(subject);
		return person;
	}
}
