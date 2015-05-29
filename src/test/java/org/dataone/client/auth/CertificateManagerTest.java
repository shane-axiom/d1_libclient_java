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
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.Security;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Map.Entry;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocket;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.v1.impl.MultipartCNode;
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
//        System.out.println("TEST SETUP: setting CertificateLocation to:" + user_cert_location);
//        CertificateManager.getInstance().setCertificateLocation(user_cert_location);
    }

    @Test
    public void testHarnessCheck() {
        assertTrue(true);
    }
    
    @Test
    public void showTLSProtocols() throws NoSuchAlgorithmException, KeyManagementException, IOException {
        
        Provider[] ps = Security.getProviders();
        for (Provider p : ps) {
            System.out.println(p.getName() + ": " + p.getClass().getCanonicalName());
            for (Entry<Object,Object> n : p.entrySet()) {
                if (n.getKey().toString().contains("SSLContext"))
                    System.out.println(String.format("    %s : %s", n.getKey(),
                            n.getValue()));
                            
            }
        }
        
        System.out.println("");
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, null, null);
        
        javax.net.ssl.SSLSocketFactory factory = (javax.net.ssl.SSLSocketFactory)ctx.getSocketFactory();
        SSLSocket engine = (SSLSocket)factory.createSocket();
        System.out.println(engine.getClass().getCanonicalName());
        
//        SSLEngine engine = ctx.createSSLEngine();
        String[] prots = engine.getSupportedProtocols();
        System.out.println("Supported Protocols: " + prots.length);
        for (String prot : prots) {
            System.out.println(" " + prot);
        }
        prots = engine.getEnabledProtocols();

        System.out.println("Enabled Protocols: " + prots.length);
        for(String prot: prots) {
             System.out.println(" " + prot);
        }
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
    public void testWildcardCert() throws BaseException, IOException, ClientSideException {
    	String[] cns = {"https://cn-dev-unm-1.test.dataone.org/cn",
    			"https://cn-dev-ucsb-1.test.dataone.org/cn",
    			"https://mn-demo-5.test.dataone.org/knb/d1/mn",
    			"https://cn-dev-orc-1.test.dataone.org/cn"};
    	for (String url : cns) {
    		System.out.println(url);
    		MultipartCNode cn = new MultipartCNode("https://cn-dev-unm-1.test.dataone.org/cn");
    		try {
    			cn.ping();
    		} catch (BaseException e) {
    			// TODO:  get getLatestRequestUrl to work again
 //   			System.out.println("Failed: " + cn.getLatestRequestUrl());
    			System.out.println("Failed: " + e.getDescription());
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
    public void testTLSPreferenceSetting_TLS_Alias() {
        
        // this "TLS" property will work with java 6,7,8 runtimes; not java 5; maybe java 9
        Settings.getConfiguration().setProperty("tls.protocol.preferences", "TLS");
        try {
            CertificateManager.getInstance().getSSLSocketFactory((String)null);
        } catch (UnrecoverableKeyException | KeyManagementException
                | KeyStoreException | CertificateException
                | NoSuchAlgorithmException | IOException e) {
            e.printStackTrace();
            fail("Threw exception when 'TLS' alias provided");
        }
    }
    
    @Test
    public void testTLSPreferenceSetting_ForwardCompatible() {
        
        // this "TLS" property will work with java 6,7,8 runtimes; not java 5; maybe java 9
        Settings.getConfiguration().setProperty("tls.protocol.preferences", "TLSv1.3, TLSv1.2, TLS");
        try {
            CertificateManager.getInstance().getSSLSocketFactory((String)null);
        } catch (UnrecoverableKeyException | KeyManagementException
                | KeyStoreException | CertificateException
                | NoSuchAlgorithmException | IOException e) {
            e.printStackTrace();
            fail("Threw exception when 'TLS' alias provided");
        } finally {
            Settings.getConfiguration().setProperty("tls.protocol.preferences", CertificateManager.defaultTlsPreferences);
        }
    }
    
    @Test
    public void testTLSPreferenceSetting_NoRealProtocols() {
        
        // this "TLS" property will work with java 6,7,8 runtimes; not java 5; maybe java 9
        Settings.getConfiguration().setProperty("tls.protocol.preferences", "foo, weboiudg");
        try {
            CertificateManager.getInstance().getSSLSocketFactory((String)null);
            fail("Didn't throw exception when only fake protocols ('foov1.2, weboiudg') were provided");
        } catch (UnrecoverableKeyException | KeyManagementException
                | KeyStoreException | CertificateException
                | NoSuchAlgorithmException | IOException e) {
            e.printStackTrace();
            
        } finally {
            Settings.getConfiguration().setProperty("tls.protocol.preferences", CertificateManager.defaultTlsPreferences);
        }
    }
    
//    @Test
    public void testSetupSSLSocketFactory() throws UnrecoverableKeyException, KeyManagementException, 
    NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException {
     
        System.out.println(SystemUtils.JAVA_RUNTIME_NAME + " " + SystemUtils.JAVA_RUNTIME_VERSION);
        System.out.println("%%%%%%%%%%%%%%%%% SSLContext Profile %%%%%%%%%%%%%%%%%%%");
        Provider[] ps = Security.getProviders();
        for (Provider p : ps) {
            System.out.println(p.getName() + ": " + p.getClass().getCanonicalName());
            for (Entry<Object,Object> n : p.entrySet()) {
                if (n.getKey().toString().contains("SSLContext"))
                    System.out.println(String.format("    %s : %s", n.getKey(),
                            n.getValue()));

            }
        }

        System.out.println("");
        SSLContext ctx = SSLContext.getInstance("TLS");
        String protocol = ctx.getProtocol();
        System.out.println(protocol);
        ctx.init(null, null, null);

        javax.net.ssl.SSLSocketFactory factory = (javax.net.ssl.SSLSocketFactory)ctx.getSocketFactory();
        SSLSocket engine = (SSLSocket)factory.createSocket();
        System.out.println("Engine impl: " + engine.getClass().getCanonicalName());

        //            SSLEngine engine = ctx.createSSLEngine();
        String[] prots = engine.getSupportedProtocols();
        System.out.println("Supported Protocols: " + prots.length);
        for (String prot : prots) {
            System.out.println(" " + prot);
        }

//        engine.setEnabledProtocols(prots);
        prots = engine.getEnabledProtocols();

        System.out.println("Enabled Protocols: " + prots.length);
        for(String prot: prots) {
            System.out.println(" " + prot);
        }
        
    	SSLSocketFactory sf = CertificateManager.getInstance().getSSLSocketFactory((String)null);
    	Scheme sch = new Scheme("https", 443, sf );
    	DefaultHttpClient hc = new DefaultHttpClient();
        hc.getConnectionManager().getSchemeRegistry().register(sch);
        
//        HttpGet req = new HttpGet("https://www.howsmyssl.com/");
//        HttpResponse response = hc.execute(req);
//        System.out.println("");
//
//        InputStream is = response.getEntity().getContent();
//        File targetFile = new File("/Users/rnahf/Documents/targetFile.html");
//        FileUtils.copyInputStreamToFile(is, targetFile);
//        String s = IOUtils.toString(is, "UTF-8");
//        int beginIndex = 0;
//        for (int i = 0; i< s.length()+80; i+=80) {
//            i = i>s.length()-1 ? s.length() : i;
//            System.out.println(s.substring(beginIndex, i));
//            beginIndex = i;
//        }
    }
    
}
