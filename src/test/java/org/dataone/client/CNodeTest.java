package org.dataone.client;

import static org.junit.Assert.assertTrue;

import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.junit.Test;

public class CNodeTest {

	@Test
	public void testLookupNodeBehavior_Null() throws ServiceFailure, NotImplemented 
	{
		new D1Client("http://cn-dev.dataone.org/cn");
		CNode cn = D1Client.getCN();
		String returnedUrl = cn.lookupNodeBaseUrl(null);
		assertTrue("null string as parameter returns null string",returnedUrl == null);
	}
	
	@Test
	public void testLookupNodeBehavior_Nonsense() throws ServiceFailure, NotImplemented 
	{
		new D1Client("http://cn-dev.dataone.org/cn");
		CNode cn = D1Client.getCN();
		String returnedUrl = cn.lookupNodeBaseUrl("foo");
		assertTrue("nonsense ID as parameter returns null string",returnedUrl == null);
	}
}
