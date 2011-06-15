package org.dataone.client.auth;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.security.cert.X509Certificate;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class CertificateManagerTest {

    private static final String user_cert_name = "/tmp/x509up_u503";
    private static final String user_pk12_name = "/tmp/x509up_u503.p12";
    private static final String user_pk12_pass = "changeitchangeit";
    private static final String CA_VALID = "cilogon-basic";
    private static final String CA_INVALID = "cilogon-silver";

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testHarnessCheck() {
        assertTrue(true);
    }
    
    @Ignore
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
        X509Certificate cert = cm.loadCertificate(user_cert_name);
        assertNotNull(cert);
        cm.displayCertificate(cert);
        
        // Verify the subject's certificate
        boolean valid = cm.verify(cert, caCert);
        assertTrue(valid);
    }
    
    @Ignore
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
        X509Certificate cert = cm.loadCertificate(user_cert_name);
        assertNotNull(cert);
        cm.displayCertificate(cert);
        
        // Verify the subject's certificate, but expect this to fail because we
        // are using the incorrect CA certificate
        boolean valid = cm.verify(cert, caCert);
        assertFalse(valid);
    }
    
    @Ignore
    @Test
    public void testPK12Loading() {
        
        // Load the manager itself
        CertificateManager cm = CertificateManager.getInstance();
        assertNotNull(cm);

        // Load the subject's certificate
        cm.setKeyStoreName(user_pk12_name);
        cm.setKeyStorePassword(user_pk12_pass);
        X509Certificate cert = cm.loadPK12Certificate();
        assertNotNull(cert);
        cm.displayCertificate(cert);
        
        // Get a certificate for the Root CA
        X509Certificate caCert = cm.getCACert(CA_VALID);
        assertNotNull(caCert);
        cm.displayCertificate(caCert);
        
        // verify
        boolean valid = cm.verify(cert, caCert);
        assertTrue(valid);
    }
}
