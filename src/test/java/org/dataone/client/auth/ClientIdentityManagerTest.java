package org.dataone.client.auth;

import static org.junit.Assert.*;

import java.io.IOException;

import org.dataone.exceptions.MarshallingException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ClientIdentityManagerTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testGetCurrentIdentity() {
		ClientIdentityManager.getCurrentIdentity();
	}

	@Test
	public final void testGetCurrentSession() throws IOException, InstantiationException, IllegalAccessException, MarshallingException {
		ClientIdentityManager.getCurrentSession();
	}

	@Ignore("not ready to test")
	@Test
	public final void testSetCurrentIdentity() {
		fail("Not yet implemented"); // TODO
	}

	@Test
	public final void testGetCertificateExpiration() {
		ClientIdentityManager.getCertificateExpiration();
	}

}
