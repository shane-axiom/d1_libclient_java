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
 */
package org.dataone.client.utils;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.concurrent.TimeUnit;

import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpException;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.AbstractHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClientBuilder;
import org.apache.http.impl.client.cache.CachingHttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.log4j.Logger;
import org.dataone.client.auth.CertificateManager;
import org.dataone.client.auth.X509Session;
import org.dataone.client.rest.HttpMultipartRestClient;
import org.dataone.configuration.Settings;
import org.dataone.exceptions.MarshallingException;

/**
 * A utility class to simplify creation and configuration of HttpClients. In an
 * effort to conform to HTTP/1.1, a PoolingConnectionManager is used, and the number
 * of parallel connections per server is adjusted to conform to current browser 
 * norms.
 * <br/>
 * @see http://www.browserscope.org/?category=network
 * @see http://www.stevesouders.com/blog/2008/03/20/roundup-on-parallel-connections/#comment-3197
 * <br/>

 * @author rnahf
 *
 */
public class HttpUtils {

	/* The instance of the logging class */
	static final Logger logger = Logger.getLogger(HttpUtils.class.getName());

	/* The name of the scheme used for SSL */
	public final static String SCHEME_NAME = "https";
	
	

	public final static boolean MONITOR_STALE_CONNECTIONS = Settings.getConfiguration()
	        .getBoolean("D1Client.http.monitorStaleConnections",true);

	/**
	 * The maximum number of connections allowed in total by the HttpClient
	 */
	public final static int MAX_CONNECTIONS = Settings.getConfiguration()
            .getInteger("D1Client.http.maxConnectionsTotal",200);

	/** 
	 * The number of parallel connections allowed per route / server 
	 * Use caution resetting this one: more is not better:
	 * @see https://redmine.dataone.org/issues/7463#note-1
	 */
    public final static int MAX_CONNECTIONS_PER_ROUTE = Settings.getConfiguration()
            .getInteger("D1Client.http.maxConnectionsPerServer",5);
    
    public final static long DEFAULT_KEEP_ALIVE_SECONDS = Settings.getConfiguration()
            .getLong("D1Client.http.keepAliveDuration.sec", 5);
    
    
    public final static boolean USE_CACHING_CLIENT = Settings.getConfiguration()
            .getBoolean("D1Client.http.useCachingClient", true);
    
    public final static int CACHE_MAX_ENTRIES = Settings.getConfiguration()
            .getInt("D1Client.http.cacheMaxEntries", 50);
    
    public final static int CACHE_MAX_OBJECT_SIZE = Settings.getConfiguration()
            .getInt("D1Client.http.cacheMaxObjectSize", 102400);
    
    
	/**
	 * Provided to assist with backwards compatibility with v4.1.x era DefaultHttpClient
	 * (now deprecated).
	 * d1_libclient_java v1.x used this method to set up SSL after creating the
	 * DefaultHttpClient, so should be used if you are working with the deprecated
	 * DefaultHttpClients.
	 * <p/>
	 * With the current CertificateManager implementation, a null value for 
	 * the session object causes the certificate at the default location to 
	 * be used.  If the certificate is not found, a warning will be logged 
	 * and SSL scheme setup will continue.
	 *  
	 * @param session
	 */
	public static void setupSSL_v4_1(AbstractHttpClient httpClient, X509Session x509Session) 
	{
		SchemeRegistry schemeReg = httpClient.getConnectionManager().getSchemeRegistry();

		if (schemeReg.getScheme(SCHEME_NAME) == null) {

			SSLSocketFactory socketFactory = null;
			try {
			    socketFactory = CertificateManager.getInstance().getSSLSocketFactory(x509Session);
			} catch (Exception e) {
				// this is likely more severe
				logger.warn("Exception from CertificateManager at SSL setup - client will be anonymous: " + 
						e.getClass() + ":: " + e.getMessage());
			}
			try {
				//443 is the default port, this value is overridden if explicitly set in the URL
				Scheme sch = new Scheme(SCHEME_NAME, 443, socketFactory );
				httpClient.getConnectionManager().getSchemeRegistry().register(sch);
			} catch (Exception e) {
				// this is likely more severe
				logger.error("Failed to set up SSL connection for client. Continuing. " + e.getClass() + ":: " + e.getMessage(), e);
			}
		}
	}

	
	/**
     * Creates an HttpClient configured with the X509 credentials.
	 * @param x509session - - the configuration object containing the X509 client certificate used for the connections
	 * @return
	 * @throws UnrecoverableKeyException
	 * @throws KeyManagementException
	 * @throws NoSuchAlgorithmException
	 * @throws KeyStoreException
	 * @throws CertificateException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IOException
	 * @throws MarshallingException
	 */
    public static HttpClient createHttpClient(X509Session x509session) throws UnrecoverableKeyException, 
    KeyManagementException, NoSuchAlgorithmException, KeyStoreException, CertificateException, 
    InstantiationException, IllegalAccessException, IOException, MarshallingException {
        return getHttpClientBuilder(x509session).build();
    }

    
    /**
     * Returns an HttpClientBuilder with the DataONE-standard ConnectionManager configuration
     * specified.  Users would further customize with additional builder methods.
     * @param x509session - the configuration object containing the X509 client certificate used for the connections
     * @return - a session-configured HttpClientBuilder
     * @throws UnrecoverableKeyException
     * @throws KeyManagementException
     * @throws NoSuchAlgorithmException
     * @throws KeyStoreException
     * @throws CertificateException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws IOException
     * @throws MarshallingException
     */
    public static HttpClientBuilder getHttpClientBuilder(X509Session x509session) throws UnrecoverableKeyException, 
    KeyManagementException, NoSuchAlgorithmException, KeyStoreException, CertificateException, 
    InstantiationException, IllegalAccessException, IOException, MarshallingException {
        return getHttpClientBuilder(x509session, null);
    }
    
    
    /**
     * Returns an HttpClientBuilder with the DataONE-standard ConnectionManager configuration
     * specified.  Users would further customize with additional builder methods.
     * @param x509session - the configuration object containing the X509 client certificate used for the connections
     * @param monitorStaleConnections - if not null chooses whether or not to monitorStaleConnections, otherwise uses
     * the default setting determined from D1Client.http.monitorStaleConnections
     * @return - a session-configured HttpClientBuilder
     * @throws UnrecoverableKeyException
     * @throws KeyManagementException
     * @throws NoSuchAlgorithmException
     * @throws KeyStoreException
     * @throws CertificateException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws IOException
     * @throws MarshallingException
     */
	public static HttpClientBuilder getHttpClientBuilder(X509Session x509session, Boolean monitorStaleConnections) throws UnrecoverableKeyException, 
	KeyManagementException, NoSuchAlgorithmException, KeyStoreException, CertificateException, 
	InstantiationException, IllegalAccessException, IOException, MarshallingException {

	    PoolingHttpClientConnectionManager connMan = new PoolingHttpClientConnectionManager(
	            buildConnectionRegistry(x509session));

	    // set timeout for hangs during connection initialization (handshakes)
	    // (these aren't handled by the RequestConfig, because happens before the request)
	    // see https://redmine.dataone.org/issues/7634
	    SocketConfig sc = SocketConfig.custom()
	            .setSoTimeout(HttpMultipartRestClient.DEFAULT_TIMEOUT_VALUE)
	            .build();

	    connMan.setDefaultSocketConfig(sc);
	    connMan.setDefaultMaxPerRoute(MAX_CONNECTIONS_PER_ROUTE);
	    connMan.setMaxTotal(MAX_CONNECTIONS);
	    
	    if (monitorStaleConnections != null) { 
	        if (monitorStaleConnections)
	            HttpConnectionMonitorService.getInstance().addMonitor(connMan);
	    }
	    else if (MONITOR_STALE_CONNECTIONS) {
	        HttpConnectionMonitorService.getInstance().addMonitor(connMan);
	    }
	    
	    return HttpUtils.selectHttpClientBuilder()
	            .setKeepAliveStrategy(buildD1KeepAliveStrategy(DEFAULT_KEEP_ALIVE_SECONDS))
	            .setConnectionManager(connMan);
	}
	
	
	/**
	 * Creates an HttpClient configured with the authorization token credentials.
	 * @param authToken - the configuration object containing the authorization token string used for the connections
	 * @return
	 */
	public static HttpClient createHttpClient(String authToken) {
	    return getHttpClientBuilder(authToken).build();
	}
	
	/**
	 * Returns an HttpClientBuilder with the DataONE-standard ConnectionManager configuration specified.
	 * Users would further customize with additional builder methods.
	 * @param authToken - the configuration object containing the authorization token string used for the connections
	 * @return
	 */
	public static HttpClientBuilder getHttpClientBuilder(final String authToken) {
	    return  getHttpClientBuilder(authToken, null);
	}
	
	/**
     * Returns an HttpClientBuilder with the DataONE-standard ConnectionManager configuration specified.
     * Users would further customize with additional builder methods.
     * @param authToken - the configuration object containing the authorization token string used for the connections
     * @param monitorStaleConnections - if not null, choose whether to monitor stale connections, otherwise
     *        use the configured default in D1Client.http.monitorStaleConnections
     * @return
     */
	public static HttpClientBuilder getHttpClientBuilder(final String authToken, Boolean monitorStaleConnections) {
	    PoolingHttpClientConnectionManager connMan = new PoolingHttpClientConnectionManager(buildConnectionRegistry());
	    
	    // set timeout for hangs during connection initialization (handshakes)
        // (these aren't handled by the RequestConfig, because happens before the request)
        // see https://redmine.dataone.org/issues/7634
	    
        SocketConfig sc = SocketConfig.custom()
                .setSoTimeout(HttpMultipartRestClient.DEFAULT_TIMEOUT_VALUE)
                .build();
        connMan.setDefaultSocketConfig(sc); 
        connMan.setDefaultMaxPerRoute(MAX_CONNECTIONS_PER_ROUTE);
        connMan.setMaxTotal(MAX_CONNECTIONS);
        
        if (monitorStaleConnections != null) { 
            if (monitorStaleConnections)
                HttpConnectionMonitorService.getInstance().addMonitor(connMan);
        }
        else if (MONITOR_STALE_CONNECTIONS) {
            HttpConnectionMonitorService.getInstance().addMonitor(connMan);
        }

        return HttpUtils.selectHttpClientBuilder()
                .setConnectionManager(connMan)
                .setKeepAliveStrategy(buildD1KeepAliveStrategy(DEFAULT_KEEP_ALIVE_SECONDS))
                .addInterceptorLast(new HttpRequestInterceptor() {

                    @Override
                    public void process(final HttpRequest request, final HttpContext context) 
                            throws HttpException, IOException 
                            {
                        request.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + authToken);
                            }
                });
	}
	
	
	private static HttpClientBuilder selectHttpClientBuilder() {
	    if (USE_CACHING_CLIENT) {
	        CachingHttpClientBuilder b = CachingHttpClients.custom();
	        // tilt the Cache to 
	        CacheConfig cacheConfig = CacheConfig.custom()
	                .setMaxCacheEntries(CACHE_MAX_ENTRIES)
	                .setMaxObjectSize(CACHE_MAX_OBJECT_SIZE)
	                .build();
            b.setCacheConfig(cacheConfig);
            return b;
	    }
	    else 
	        return HttpClients.custom();
	}
	
//    public static Registry<ConnectionSocketFactory> buildConnectionRegistry(String subjectString) 
//    throws UnrecoverableKeyException, KeyManagementException, NoSuchAlgorithmException, 
//    KeyStoreException, CertificateException, IOException, InstantiationException,
//    IllegalAccessException, MarshallingException {
//
//        X509Session x = CertificateManager.getInstance().selectSession(subjectString);
//        return buildConnectionRegistry(x);
//    }
    
    public static X509Session selectSession(String subjectString) throws IOException {
        return CertificateManager.getInstance().selectSession(subjectString);
    }
	
	public static Registry<ConnectionSocketFactory> buildConnectionRegistry(X509Session x509Session) 
	        throws UnrecoverableKeyException, KeyManagementException, NoSuchAlgorithmException, 
	        KeyStoreException, CertificateException, IOException {
    
        RegistryBuilder<ConnectionSocketFactory> rb = RegistryBuilder.<ConnectionSocketFactory>create();
        rb.register("http", PlainConnectionSocketFactory.getSocketFactory());

        LayeredConnectionSocketFactory sslSocketFactory = null;
        sslSocketFactory = CertificateManager.getInstance().getSSLConnectionSocketFactory(x509Session);

        rb.register("https", sslSocketFactory);

        Registry<ConnectionSocketFactory> sfRegistry = rb.build();
        return sfRegistry;
    }
	
	public static Registry<ConnectionSocketFactory> buildConnectionRegistry() 
	{
	    RegistryBuilder<ConnectionSocketFactory> rb = RegistryBuilder.<ConnectionSocketFactory>create();
	    rb.register("http", PlainConnectionSocketFactory.getSocketFactory());

	    LayeredConnectionSocketFactory sslSocketFactory = null;
	    sslSocketFactory = SSLConnectionSocketFactory.getSocketFactory();

	    rb.register("https", sslSocketFactory);

	    Registry<ConnectionSocketFactory> sfRegistry = rb.build();
	    return sfRegistry;
	}
	
	/**
	 * Sets the CONNECTION_TIMEOUT and SO_TIMEOUT values for the request.
	 * (max delay in initial response, max delay between tcp packets, respectively).  
	 * Uses the same value for both.
	 * 
	 * @param defaultRestClient - the MultipartRestClient implementation
	 * @param milliseconds
	 * @return the previous RequestConfig associated with the HttpMultipartRestClient
	 */
//	public static RequestConfig setTimeouts(MultipartRestClient rc, int milliseconds) 
//	{
//        Integer timeout = new Integer(milliseconds);
//        
//        RequestConfig previous = null;
//        
//        if (rc instanceof HttpMultipartRestClient) {
//        	
//        	previous = ((HttpMultipartRestClient) rc).getRequestConfig();
//        	
//        	// as of httpClient v4.3.x
//        	RequestConfig requestConfig = RequestConfig.custom()
//        			.setConnectTimeout(timeout)
//        			.setConnectionRequestTimeout(timeout)
//        			.setSocketTimeout(timeout)
//        			.build();
//        	((HttpMultipartRestClient) rc).setRequestConfig(requestConfig);
//        	
//        	
//// for httpClient v4.1.x 
////        	HttpClient hc = ((HttpMultipartRestClient) rc).getHttpClient();
////        
////        	HttpParams params = hc.getParams();
////        	// the timeout in milliseconds until a connection is established.
////        	HttpConnectionParams.setConnectionTimeout(params, timeout);
////        
////        	//defines the socket timeout (SO_TIMEOUT) in milliseconds, which is the timeout
////        	// for waiting for data or, put differently, a maximum period inactivity between
////        	// two consecutive data packets).
////        	HttpConnectionParams.setSoTimeout(params, timeout);
//// 
////        	
////        	((DefaultHttpClient)rc).setParams(params);
//        }
//        return previous;
//	}
	/**
	 * Builds a custom keep-alive strategy that uses the defaultKeepAliveTimeout
	 * value if one not returned in the response headers.  (This allows us to
	 * expire idle connections).
	 * 
	 * @param defaultKeepAliveTimeout - number of seconds (to match units of the keep-alive timeout header) 
	 * @return
	 */
	public static ConnectionKeepAliveStrategy buildD1KeepAliveStrategy(final long defaultKeepAliveTimeout) {
	    return new ConnectionKeepAliveStrategy() {

	        @Override
	        public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
	            // Honor 'keep-alive' header
	            HeaderElementIterator it = new BasicHeaderElementIterator(
	                    response.headerIterator(HTTP.CONN_KEEP_ALIVE));
	            while (it.hasNext()) {
	                HeaderElement he = it.nextElement();
	                String param = he.getName();
	                String value = he.getValue();
	                if (value != null && param.equalsIgnoreCase("timeout")) {
	                    try {
	                        return Long.parseLong(value) * 1000;
	                    } catch(NumberFormatException ignore) {
	                    }
	                }
	            }
	            // no keep-alive returned from the response
	            return defaultKeepAliveTimeout * 1000;
	        }
	    };
	    }

}
