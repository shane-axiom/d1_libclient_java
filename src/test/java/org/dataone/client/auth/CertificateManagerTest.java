package org.dataone.client.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import org.apache.http.conn.ssl.SSLSocketFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SubjectInfo;
import org.jibx.runtime.JiBXException;
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
    
    @Ignore("will not pass until certificates installed on Hudson")
    @Test
    public void testCertificateManager() {
        
        // Load the manager itself
        CertificateManager cm = CertificateManager.getInstance();
        assertNotNull(cm);
        
        // Get a certificate for the Root CA
        X509Certificate caCert = cm.getCACert(CA_VALID);
        assertNotNull(caCert);
        cm.displayCertificate(caCert);

        // Load the subject's certificate
        X509Certificate cert = cm.loadCertificate();
        assertNotNull(cert);
        cm.displayCertificate(cert);
        
        // Verify the subject's certificate
        boolean valid = cm.verify(cert, caCert);
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
			SSLSocketFactory ssl = CertificateManager.getInstance().getSSLSocketFactory(session);
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
        boolean valid = cm.verify(cert, caCert);
        assertFalse(valid);
    }
    
}
