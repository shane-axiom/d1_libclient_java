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
package org.dataone.client.impl.rest;

import org.apache.http.client.HttpClient;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.dataone.client.auth.CertificateManager;
import org.dataone.client.utils.HttpUtils;
import org.dataone.service.types.v1.Session;

public class DefaultHttpMultipartRestClient extends HttpMultipartRestClient {

	public DefaultHttpMultipartRestClient(Integer timeoutSeconds) 
	{	
		super(buildHttpClient(null),null);
		HttpUtils.setTimeouts(this, timeoutSeconds*1000);
	}

	private static HttpClient buildHttpClient(Session session)  
	{		
		RegistryBuilder<ConnectionSocketFactory> rb = RegistryBuilder.<ConnectionSocketFactory>create();		
		rb.register("http", PlainConnectionSocketFactory.getSocketFactory());

		try {
			String subjectString = null;
			if (session != null && session.getSubject() != null) {
				subjectString = session.getSubject().getValue();
			}
					
			LayeredConnectionSocketFactory sslSocketFactory = 
					CertificateManager.getInstance().getSSLConnectionSocketFactory(subjectString);
			rb.register("https", sslSocketFactory);
		} 
		catch (Exception e) {
			// this is likely more severe
			log.warn("Exception from CertificateManager at SSL setup - client will be anonymous: " + 
					e.getClass() + ":: " + e.getMessage());
			
		}
		 
		Registry<ConnectionSocketFactory> sfRegistry = rb.build();

		HttpClientConnectionManager connMan = new PoolingHttpClientConnectionManager(sfRegistry);
//		HttpClientConnectionManager connMan = new BasicHttpClientConnectionManager(sfRegistry);
		HttpClient hc = HttpClients.custom()
				.setConnectionManager(connMan)
				.build();
			
		return hc;
	}
	
}
