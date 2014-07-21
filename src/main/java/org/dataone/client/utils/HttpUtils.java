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

import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.dataone.client.auth.CertificateManager;
import org.dataone.service.types.v1.Session;

public class HttpUtils {

	/* The instance of the logging class */
	private static Logger logger = Logger.getLogger(HttpUtils.class.getName());

	/* The name of the scheme used for SSL */
	public final static String SCHEME_NAME = "https";
	
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
	public static void setupSSL_v4_1(DefaultHttpClient httpClient, Session session) 
	{		
		SchemeRegistry schemeReg = httpClient.getConnectionManager().getSchemeRegistry();

		if (schemeReg.getScheme(SCHEME_NAME) == null) {

			SSLSocketFactory socketFactory = null;
			try {
				String subjectString = null;
				if (session != null && session.getSubject() != null) {
					subjectString = session.getSubject().getValue();
				}
				socketFactory = CertificateManager.getInstance().getSSLSocketFactory(subjectString);
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
	
	
	
	public static Registry<ConnectionSocketFactory> buildConnectionRegistry(Session session) 
    throws UnrecoverableKeyException, KeyManagementException, NoSuchAlgorithmException, 
    KeyStoreException, CertificateException, IOException 
    {
		RegistryBuilder<ConnectionSocketFactory> rb = RegistryBuilder.<ConnectionSocketFactory>create();		
		rb.register("http", PlainConnectionSocketFactory.getSocketFactory());


		String subjectString = null;
		if (session != null && session.getSubject() != null) {
			subjectString = session.getSubject().getValue();
		}

		LayeredConnectionSocketFactory sslSocketFactory = 
				CertificateManager.getInstance().getSSLConnectionSocketFactory(subjectString);
		rb.register("https", sslSocketFactory);
		 
		Registry<ConnectionSocketFactory> sfRegistry = rb.build();
		return sfRegistry;
		
	}
	
	/**
	 * Sets the CONNECTION_TIMEOUT and SO_TIMEOUT values for the request.
	 * (max delay in initial response, max delay between tcp packets, respectively).  
	 * Uses the same value for both.
	 * 
	 * @param restClient - the MultipartRestClient implementation
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
    
    
}
