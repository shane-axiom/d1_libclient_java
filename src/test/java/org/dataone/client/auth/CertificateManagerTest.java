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

package org.dataone.client.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import org.apache.http.conn.ssl.SSLSocketFactory;
import org.dataone.client.CNode;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SubjectInfo;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class CertificateManagerTest {

    private static final String user_cert_location = Settings.getConfiguration().getString("certificate.location");
    
    private static final String CA_VALID = "cilogon-basic";
    private static final String CA_INVALID = "cilogon-silver";
    

    @Before
    public void setUp() throws Exception {
    	// can override the location of the PEM cert
    	CertificateManager.getInstance().setCertificateLocation(user_cert_location);
    }

    @Test
    public void testHarnessCheck() {
        assertTrue(true);
    }
    
    
    @Test
    public void testTrustManager() {
    	
        // Load the manager itself
        CertificateManager cm = CertificateManager.getInstance();
        assertNotNull(cm);
        
        // Get a certificate for the Root CA
        X509Certificate caCert = cm.getCACert("cn=dataone root ca,dc=dataone,dc=org");
        assertNotNull(caCert);
        System.out.println(caCert.getSubjectDN());
        //cm.displayCertificate(caCert);	
    }
    
   // this was a temporary test to test against a real SSL handshake - if necessary
    // move it to d1_integration
//    @Test
    public void testWildcardCert() throws BaseException {
    	String[] cns = {"https://cn-dev-unm-1.test.dataone.org/cn",
    			"https://cn-dev-ucsb-1.test.dataone.org/cn",
    			"https://mn-demo-5.test.dataone.org/knb/d1/mn",
    			"https://cn-dev-orc-1.test.dataone.org/cn"};
    	for (String url : cns) {
    		System.out.println(url);
    		CNode cn = new CNode("https://cn-dev-unm-1.test.dataone.org/cn");
    		try {
    			cn.ping();
    		} catch (BaseException e) {
    			System.out.println("Failed: " + cn.getLatestRequestUrl());
    		}
    	}
    	
    	
    }
    
    
    @Ignore("will not pass until certificates installed on Hudson")
    @Test
    public void testCertificateManager() {
        
        // Load the manager itself
        CertificateManager cm = CertificateManager.getInstance();
        assertNotNull(cm);
        
        // Get a certificate for the Root CA
        X509Certificate caCert = cm.getCACert("cn=dataone root ca,dc=dataone,dc=org");
        assertNotNull(caCert);
        cm.displayCertificate(caCert);

        // Load the subject's certificate
        X509Certificate cert = cm.loadCertificate();
        assertNotNull(cert);
        cm.displayCertificate(cert);
        
        // Verify the subject's certificate
        boolean valid = CertificateManager.verify(cert, caCert);
        assertTrue(valid);
    }
 
    
    
    
    
    @Ignore("will not pass until certificates installed on Hudson")
    @Test
    public void testCertificateManagerExtension() {
    	try {
	        // Load the subject's certificate
	        X509Certificate cert = CertificateManager.getInstance().loadCertificate();
	        assertNotNull(cert);
			SubjectInfo subjectInfo = CertificateManager.getInstance().getSubjectInfo(cert);
			if (subjectInfo != null) {
				// the certificate should match the first person in the subjectInfo
				String serviceSubject = CertificateManager.getInstance().standardizeDN(subjectInfo.getPerson(0).getSubject().getValue());
				String certSubject = CertificateManager.getInstance().getSubjectDN(cert);
				System.out.println("Subject from certificate extension: " + serviceSubject);
				assertEquals(serviceSubject, certSubject);
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		} 
        
       
    }
    
    @Ignore("will not pass until certificates installed on Hudson")
    @Test
    public void testCustomCertificateManager() {
        
        // Get a certificate for the Root CA
        X509Certificate caCert = CertificateManager.getInstance().getCACert(CA_VALID);
        assertNotNull(caCert);
        CertificateManager.getInstance().displayCertificate(caCert);

        // Load the subject's certificate
        X509Certificate certificate = CertificateManager.getInstance().loadCertificate();
        assertNotNull(certificate);
        CertificateManager.getInstance().displayCertificate(certificate);
        
        // get the private key
        PrivateKey key = CertificateManager.getInstance().loadKey();
        
        // register as the subject
        String subjectDN = CertificateManager.getInstance().getSubjectDN(certificate);
        CertificateManager.getInstance().registerCertificate(subjectDN, certificate, key);
        
        // load the SSL for custom Session
        Session session = new Session();
        Subject subject = new Subject();
        subject.setValue(subjectDN);
		session.setSubject(subject );
        try {
			SSLSocketFactory ssl = CertificateManager.getInstance().getSSLSocketFactory(session.getSubject().getValue());
			assertNotNull(ssl);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
        
    }
    
    @Ignore("will not pass untilcertificates installed on Hudson")
    @Test
    public void testIncorrectCA() {
        
        // Load the manager itself
        CertificateManager cm = CertificateManager.getInstance();
        assertNotNull(cm);
        
        // Get a certificate for the Root CA
        X509Certificate caCert = cm.getCACert(CA_INVALID);
        assertNotNull(caCert);
        cm.displayCertificate(caCert);

        // Load the subject's certificate
        X509Certificate cert = cm.loadCertificate();
        assertNotNull(cert);
        cm.displayCertificate(cert);
        
        // Verify the subject's certificate, but expect this to fail because we
        // are using the incorrect CA certificate
        boolean valid = CertificateManager.verify(cert, caCert);
        assertFalse(valid);
    }
    
    @Test
    public void testStandardizeSubjectDN() {
    	try {
    		// different permutations on the same subject
    		String dn1 = "CN=test,DC=dataone,DC=org";
    		String dn2 = "cn=test,dc=dataone,dc=org";
    		String dn3 = "CN=test, DC=dataone, DC=org";
    		String dn4 = "DC=org, DC=dataone, CN=test";
    		
    		// d1 == d2
    		assertEquals(
    				dn1, 
    				CertificateManager.getInstance().standardizeDN(dn2));
    		// d1 == d3
    		assertEquals(
    				dn1, 
    				CertificateManager.getInstance().standardizeDN(dn3));
    		// d1 != d4
    		assertFalse(
    				CertificateManager.getInstance().standardizeDN(dn1).equals( 
    				CertificateManager.getInstance().standardizeDN(dn4)));
    		
    		// equalsDN method
    		assertTrue(
    				CertificateManager.getInstance().equalsDN(dn1, dn2));
    		
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}    
    }
    
    @Test
    public void testDecodeSubjectDN() {
    	try {
    		// different special characters encoded using UTF-7
    		String dn1 = "CN=Fl\\+AOE-via Pezzini T6821,O=Google,C=US,DC=cilogon,DC=org"; // Flávia Pezzini
    		String dn1Expected = "CN=Fl\\+AOE-via Pezzini T6821,O=Google,C=US,DC=cilogon,DC=org";
    		String dn1Incorrect = "CN=Flávia Pezzini T6821,O=Google,C=US,DC=cilogon,DC=org";

    		String dn2 = "CN=\\+aQVbUA-,O=Google,C=US,DC=cilogon,DC=org"; //椅子 -- "chair" in Chinese
    		String dn2Expected = "CN=\\+aQVbUA-,O=Google,C=US,DC=cilogon,DC=org";
    		String dn2Incorrect = "CN=椅子,O=Google,C=US,DC=cilogon,DC=org";
    		
    		// dn1 = expected
    		assertEquals(
    				dn1Expected, 
    				CertificateManager.getInstance().standardizeDN(dn1));
    		// dn2 == expected
    		assertEquals(
    				dn2Expected, 
    				CertificateManager.getInstance().standardizeDN(dn2));
    		// no decoding should be performed
    		assertFalse(
    				CertificateManager.getInstance().standardizeDN(dn1).equals( 
    				CertificateManager.getInstance().standardizeDN(dn1Incorrect)));
    		// equalsDN method
    		assertTrue(
    				CertificateManager.getInstance().equalsDN(CertificateManager.getInstance().standardizeDN(dn1), dn1Expected));
    		// equalsDN method
    		assertFalse(
    				CertificateManager.getInstance().equalsDN(dn2, dn2Incorrect));
    		
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}    
    }
    
    
    /**
     * tests that exception thrown when a bad subject value is passed in
     */
    @Test
    public void testGetSSLSocketFactory_badSubjectValue() {
    	try {
    		CertificateManager.getInstance().getSSLSocketFactory("blah_blah");
    	} catch (KeyStoreException e) {
    		// this is what we expect
    	} catch (Exception e) {
    		e.printStackTrace();
    		fail();
    	} 
    }
    
    @Test
    public void testLocateDefaultCertificate() {
    	try {
    		File f = CertificateManager.getInstance().locateDefaultCertificate();
    		System.out.println("Default Certificate Loation: " + f.getAbsolutePath());
    		assertTrue(f.exists());
    		String userTmpDir = (System.getProperty("tmpdir") == null) ? "/tmp" : System.getProperty("tmpdir");
    		System.out.println("user tempDir: " + userTmpDir);
    		assertTrue(f.getAbsolutePath().startsWith(userTmpDir + "/x509up_u"));
    		
    	} catch (FileNotFoundException e) {
    		// ok
    	}
    }
    
    @Test
    public void testSetupSSLSocketFactory() throws UnrecoverableKeyException, KeyManagementException, 
    NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException {
    	CertificateManager.getInstance().getSSLSocketFactory(null);
    }
    
}
