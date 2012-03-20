package org.dataone.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Group;
import org.dataone.service.types.v1.Person;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SubjectList;
import org.dataone.service.util.D1Url;
import org.junit.Test;

public class CNodeTest {

	@Test
	public void testFoo() {
		
	}
	
	/**
	 * this test is needed to validate the hack used in the cn.search() convenience
	 * method that uses a D1Url object instead of string to build the 'query' parameter.
	 */
	@Test
	public void testD1UrlHack() {
		D1Url d1url = new D1Url("base","resource");
		d1url.addNonEmptyParamPair("jumbo","shrimp");
		String beyondResource = d1url.getUrl().replaceAll("^base/resource/{0,1}", "");
		assertEquals("these should match", beyondResource, "?jumbo=shrimp");

		d1url.addNextPathElement("path1");
		d1url.addNextPathElement("path2");
		beyondResource = d1url.getUrl().replaceAll("^base/resource/{0,1}", "");
		assertEquals("these should match", beyondResource, "path1/path2?jumbo=shrimp");		
	}
	
	//@Test
	public void testLookupNodeBehavior_Null() throws ServiceFailure, NotImplemented 
	{
		CNode cn = new CNode("");
		String returnedUrl = cn.lookupNodeBaseUrl((String) null);
		assertTrue("null string as parameter returns null string",returnedUrl == null);
	}
	
//	@Test
	public void testLookupNodeBehavior_Nonsense() throws ServiceFailure, NotImplemented 
	{
		CNode cn = new CNode("");
		String returnedUrl = cn.lookupNodeBaseUrl("foo");
		assertTrue("nonsense ID as parameter returns null string",returnedUrl == null);
	}
	
//	@Test
	public void reuseableTest() throws Exception 
	{
		
		//Settings.getConfiguration().setProperty("D1Client.CN_URL", "http://localhost:8080/cn");
		Settings.getConfiguration().setProperty("D1Client.CN_URL", "https://cn-dev.dataone.org/cn");
		
		Subject subject = new Subject();
		//subject.setValue("CN=Benjamin Leinfelder A458,O=University of Chicago,C=US,DC=cilogon,DC=org");
		subject.setValue("CN=Dave Vieglais T480,O=Google,C=US,DC=cilogon,DC=org");
		
		Person person = new Person();
		person.setSubject(subject);
		person.setFamilyName("test1");
		person.addGivenName("test1");
		person.addEmail("ben@d1.org");

		// register
		//D1Client.getCN().registerAccount(null, person);
		// now update
		//person.setFamilyName("test2");
		//D1Client.getCN().updateAccount(null, person);

		// group
		SubjectList members = new SubjectList();
		members.addSubject(subject);
		Subject groupSubject = new Subject();
		groupSubject.setValue("CN=testGroup,DC=cilogon,DC=org");
		Group group = new Group();
		group.setSubject(groupSubject);
		D1Client.getCN().updateGroup(null, group);


	}
	
}
