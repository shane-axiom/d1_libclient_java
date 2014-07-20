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
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.dataone.client.utils.HttpUtils;
import org.dataone.service.types.v1.Session;


public class DefaultHttpMultipartRestClient extends HttpMultipartRestClient {

	public DefaultHttpMultipartRestClient() 
	{	
		super(buildHttpClient(null));
	}
	
	public DefaultHttpMultipartRestClient(Session session) 
	{
		super(buildHttpClient(session));
	}

	
	private static HttpClient buildHttpClient(Session session)  
	{		
		
		Registry<ConnectionSocketFactory> sfRegistry = null;
		try {
			sfRegistry = HttpUtils.buildConnectionRegistry(session);
		} 
		catch (Exception e) {
			// this is likely more severe
			log.warn("Exception from CertificateManager at SSL setup - client will be anonymous: " + 
					e.getClass() + ":: " + e.getMessage());
			
		}

		HttpClientConnectionManager connMan = new PoolingHttpClientConnectionManager(sfRegistry);
		HttpClient hc = HttpClients.custom()
				.setConnectionManager(connMan)
				.build();
			
		return hc;
	}
	
}
