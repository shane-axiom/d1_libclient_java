package org.dataone.client.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.config.RequestConfig;
import org.dataone.client.auth.CertificateManager;
import org.dataone.client.auth.X509Session;
import org.dataone.client.exception.ClientSideException;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Session;
import org.junit.Before;
import org.junit.Test;

public class HttpMultipartRestClientTest {

    protected static Log log = LogFactory.getLog(HttpMultipartRestClientTest.class);
    
    private Method method;
    private HttpMultipartRestClient restClient;

    
    @Before
    public void setup() throws NoSuchMethodException, SecurityException, IOException, ClientSideException {
        
        // testing a private method, so ... reflection!
        restClient = new HttpMultipartRestClient();
        method = HttpMultipartRestClient.class.getDeclaredMethod("determineRequestConfig", Integer.class, Boolean.class);
        method.setAccessible(true);
    }
    
    @Test 
    public void testConstructUsingRegisteredCertificate() throws IOException, ClientSideException {

        CertificateManager cm = CertificateManager.getInstance();

        URL url = Thread.currentThread().getContextClassLoader()
                .getResource("org/dataone/client/rest/unitTestSelfSignedCert.pem");
        cm.setCertificateLocation(url.getPath());
        PrivateKey pk = cm.loadKey();

        X509Certificate xc = cm.loadCertificate();
        String subjectString = cm.getSubjectDN(xc);

        cm.registerCertificate(subjectString, xc, pk);

        System.out.println("starting certificate Subject Value " + subjectString);
        cm.displayCertificate(xc);

        HttpMultipartRestClient hmrc = new HttpMultipartRestClient(subjectString);
        System.out.println("resulting certificate Subject Value " + cm.getSubjectDN(hmrc.getSession().getCertificate()));
    }
    
    @Test
    public void testDefaultHttpMultipartRestClient() throws IOException, ClientSideException {

        CertificateManager cm = CertificateManager.getInstance();
        
        // create the DefaultHttpMultipartRestClient
        DefaultHttpMultipartRestClient hmrc = new DefaultHttpMultipartRestClient();
        // get it's Session information (certificate information)
        X509Session initialSession = hmrc.getSession();
        String initialSubjectDN = null;
        if (initialSession != null && initialSession.getCertificate() != null) {
            System.out.println("resulting certificate Subject Value " + cm.getSubjectDN(initialSession.getCertificate()));
            initialSubjectDN = cm.getSubjectDN(initialSession.getCertificate());
        } else {
            System.out.println("resulting certificate Subject Value is null");
        }
        // set a new default certificateLocation
        URL certUrl = Thread.currentThread().getContextClassLoader()
                .getResource("org/dataone/client/rest/unitTestSelfSignedCert.pem");
        cm.setCertificateLocation(certUrl.getPath());
        System.out.println("new cert location: " + certUrl.getPath());
        // it should trigger an update in the DefaultHttpMRC to get a new HttpClient
        X509Session newSession = hmrc.getSession();
        assertNotNull("newSession should not be null", newSession);
        assertNotNull("newSession's certificate should not be null",newSession.getCertificate());
        String newSubjectDN = cm.getSubjectDN(newSession.getCertificate());
        System.out.println("new Session Cert's SubjectDN: " + newSubjectDN);
        assertTrue("The new session's SubjectDNs should be different.", newSubjectDN != initialSubjectDN);

    }


    @Test
    public void testDetermineRequestConfig() throws IllegalAccessException, 
    IllegalArgumentException, InvocationTargetException {
        
        RequestConfig requestConfig = null;
        
        // both params are null
        requestConfig = (RequestConfig) method.invoke(restClient, null, null);
        assertNotNull("RequestConfig should not be null if input parameters are null", requestConfig);
        
        // timeouts are null, redirect is valid
        Settings.getConfiguration().clearProperty(HttpMultipartRestClient.DEFAULT_TIMEOUT_PARAM);
        requestConfig = (RequestConfig) method.invoke(restClient, null, true);
        assertEquals("Unspecified connect timeout should default to -1", requestConfig.getConnectTimeout(), -1);
        assertEquals("Unspecified connection request timeout should default to -1", requestConfig.getConnectionRequestTimeout(), -1);
        assertEquals("Unspecified socket timeout should default to -1", requestConfig.getSocketTimeout(), -1);
        assertNotNull("RequestConfig shouldn't be null if followRedirect parameter is valid", requestConfig);
        assertTrue("Redirect should be enabled", requestConfig.isRedirectsEnabled());
        
        // timeouts are valid, redirect is null
        requestConfig = (RequestConfig) method.invoke(restClient, 1000, null);
        assertNotNull("RequestConfig shouldn't be null if timeoutMillis parameter is valid", requestConfig);
        assertEquals("Connect timeout should be 1000", requestConfig.getConnectTimeout(), 1000);
        assertEquals("Connection request timeout should be 1000", requestConfig.getConnectionRequestTimeout(), 1000);
        assertEquals("Socket timeout should be 1000", requestConfig.getSocketTimeout(), 1000);
        assertTrue("When not explicitly set, redirect defaults to true", requestConfig.isRedirectsEnabled());
        
        // redirect enabled
        requestConfig = (RequestConfig) method.invoke(restClient, 1000, true);
        assertTrue("Redirect should be enabled", requestConfig.isRedirectsEnabled());
        
        // redirect disabled
        requestConfig = (RequestConfig) method.invoke(restClient, 1000, false);
        assertFalse("Redirect should be disabled", requestConfig.isRedirectsEnabled());
        
        // timeouts are zero
        requestConfig = (RequestConfig) method.invoke(restClient, 0, true);
        assertEquals("Connect timeout should be 0", requestConfig.getConnectTimeout(), 0);
        assertEquals("Connection request timeout should be 0", requestConfig.getConnectionRequestTimeout(), 0);
        assertEquals("Socket timeout should be 0", requestConfig.getSocketTimeout(), 0);
        
        // timeouts are negative
        requestConfig = (RequestConfig) method.invoke(restClient, -1000, true);
        assertEquals("Connect timeout should be 0", requestConfig.getConnectTimeout(), -1000);
        assertEquals("Connection request timeout should be 0", requestConfig.getConnectionRequestTimeout(), -1000);
        assertEquals("Socket timeout should be 0", requestConfig.getSocketTimeout(), -1000);
    
        // both params are null, check default timeout setting
        Settings.getConfiguration().setProperty("D1Client.default.timeout", 2000);
        Integer defaultTimeout = Settings.getConfiguration().getInteger(HttpMultipartRestClient.DEFAULT_TIMEOUT_PARAM, null);
        assertEquals("Default timeout should've been set correctly", defaultTimeout.intValue(), 2000);
        requestConfig = (RequestConfig) method.invoke(restClient, null, null);
        assertNotNull("RequestConfig should not be null if input parameters are null", requestConfig);
        assertEquals("Default connect timeout should be 2000", requestConfig.getConnectTimeout(), 2000);
        assertEquals("Default connection request timeout should be 2000", requestConfig.getConnectionRequestTimeout(), 2000);
        assertEquals("Default socket timeout should be 2000", requestConfig.getSocketTimeout(), 2000);
    
//      
    
    }
}
