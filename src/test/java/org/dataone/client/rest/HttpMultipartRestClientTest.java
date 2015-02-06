package org.dataone.client.rest;

import static org.junit.Assert.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.dataone.client.exception.ClientSideException;
import org.junit.Before;
import org.junit.Test;

public class HttpMultipartRestClientTest {

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
    public void testDetermineRequestConfig() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        
        RequestConfig requestConfig = null;
        
        // both params are null
        requestConfig = (RequestConfig) method.invoke(restClient, null, null);
        assertNull("RequestConfig should be null if input parameters are null", requestConfig);
        
        // timeouts are null, redirect is valid
        requestConfig = (RequestConfig) method.invoke(restClient, null, true);
        assertEquals("Unspecified connect timeout should default to -1", requestConfig.getConnectTimeout(), -1);
        assertEquals("Unspecified connection request timeout should default to -1a", requestConfig.getConnectionRequestTimeout(), -1);
        assertEquals("Unspecified socket timeout should default to -1a", requestConfig.getSocketTimeout(), -1);
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
    }
}
