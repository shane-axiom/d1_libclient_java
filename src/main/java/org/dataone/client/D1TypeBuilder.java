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
		//		// thrown by IndernetAddress constructor
		//	}

		person.addEmail(emailString);
		person.addGivenName(givenName);
		person.setFamilyName(familyName);
		person.setSubject(subject);
		return person;
	}
}
