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

package org.dataone.client;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.dataone.client.auth.CertificateManager;
import org.dataone.mimemultipart.SimpleMultipartEntity;
import org.dataone.service.Constants;
import org.dataone.service.EncodingUtilities;



/**
 * A generic client class that contains base functionality for making REST calls
 * to remote REST servers.  
 * It is built to encapsulate the communication conventions dataONE is following
 * but does not know about dataone objects (see D1RestClient for that)
 * */
public class RestClient {

	protected static Log log = LogFactory.getLog(RestClient.class);
	
    /** The URL string for the node REST API */
    private DefaultHttpClient httpClient;
    private HashMap<String, String> headers = new HashMap<String, String>();
    
	/**
	 * Constructor to create a new instance.
	 */
	public RestClient() {
	    httpClient = new DefaultHttpClient();
	    setupSSL();
	}
	
	
	public void setupSSL() {
		
			try {
				SSLSocketFactory socketFactory = CertificateManager.getInstance().getSSLSocketFactory();
				Scheme sch = new Scheme("https", socketFactory, 443);
	            httpClient.getConnectionManager().getSchemeRegistry().register(sch);
			} catch (FileNotFoundException e) {
				// these are somewhat expected for anonymous d1 client use
				log.warn("Could not set up SSL connection for client - likely because the certificate could not be located: " + e.getMessage());
			} catch (Exception e) {
				// this is likely more severe
				log.error("Failed to set up SSL connection for client: " + e.getMessage(), e);
			}
            
		
	}

    // ==========================  New Handlers ===========================//

    /**
     * assembles url components into a properly encoded complete url
     */
	// TODO: remove this?  now that d1_common_java.D1Url class is created
	public static String assembleAndEncodeUrl(String baseURL, String resource, 
			String[] pathElements, Map<String,Object> queryParams) {
		
		if (baseURL == null)
			throw new IllegalArgumentException("baseURL parameter cannot be null");
		
		String url = baseURL; 
		if (!url.endsWith("/"))
			url += "/";
		url += resource;
		
		for(int i=0;i<pathElements.length;i++) 
			url += "/" + EncodingUtilities.encodeUrlPathSegment(pathElements[i]);

		
		Set<String> paramKeys = queryParams.keySet();
		Iterator<String> it = paramKeys.iterator();
		Vector<String> params = new Vector<String>();
		StringBuffer item = new StringBuffer(512);
		while (it.hasNext()) {
			String k = it.next();
			if (k == "") continue;
			
			item.append(EncodingUtilities.encodeUrlQuerySegment(k));
			queryParams.get(k);
			String val = (String) queryParams.get(k);
			if (val != null)
				if (val != "") {
					item.append("=");
					item.append(EncodingUtilities.encodeUrlQuerySegment(val));
				}
			if (it.hasNext())
				item.append("&");
			
			params.add(item.toString());
		}
		// validated parameters
		Iterator<String> it2 = params.listIterator();
		if (it2.hasNext())
			url += "?";
		while (it2.hasNext()) {
			url += it2.next();
		}
		return url;
	}
	
	public void setHeader(String name, String value) {
		headers.put(name, value);
	}
	
	public HashMap<String, String> getAddedHeaders() {
		return headers;
	}
	
	
    /**
	 * send a GET request to the resource and get the response
     * @throws IOException 
     * @throws ClientProtocolException  
	 */
	public HttpResponse doGetRequest(String url) throws ClientProtocolException, IOException  {
		return doRequestNoBody(url,Constants.GET);
	}

    /**
	 * send a Head request to the resource and get the response
     * @throws IOException 
     * @throws ClientProtocolException 
	 */

	public HttpResponse doHeadRequest(String url) throws ClientProtocolException, IOException  {
		return doRequestNoBody(url,Constants.HEAD);
	}

    /**
	 * send a Delete request to the resource and get the response
     * @throws IOException 
     * @throws ClientProtocolException 
	 */

	public HttpResponse doDeleteRequest(String url) throws ClientProtocolException, IOException {
		return doRequestNoBody(url,Constants.DELETE);
	}
	
	/**
	 * send a body-containing Delete request to the resource and get the response
     * @throws IOException 
     * @throws ClientProtocolException 
	 */

	public HttpResponse doDeleteRequest(String url, SimpleMultipartEntity mpe) throws ClientProtocolException, IOException {
		return doRequestMMBody(url,Constants.DELETE, mpe);
	}
	
    /**
	 * send a POST request to the resource and get the response
     * @throws IOException 
     * @throws ClientProtocolException  
	 */
	public HttpResponse doPostRequest(String url, SimpleMultipartEntity mpe) throws ClientProtocolException, IOException   {
		return doRequestMMBody(url,Constants.POST,mpe);
	}

    /**
	 * send a PUT request to the resource and get the response
     * @throws IOException 
     * @throws ClientProtocolException 
	 */
	public HttpResponse doPutRequest(String url, SimpleMultipartEntity mpe) throws ClientProtocolException, IOException  {
		return doRequestMMBody(url,Constants.PUT,mpe);
	}
	

	/*
	 * assembles the request for GETs, HEADs and DELETEs - assumes no message body
	 */
	private HttpResponse doRequestNoBody(String url,String httpMethod) throws ClientProtocolException, IOException  {
		log.info("restURL: " + url);
		log.info("method: " + httpMethod);

		HttpUriRequest req = null;
		if (httpMethod == Constants.GET) 
			req = new HttpGet(url);        	
		else if (httpMethod == Constants.HEAD) 
			req = new HttpHead(url);       
		else if (httpMethod == Constants.DELETE)
			req = new HttpDelete(url);       
		else 
			throw new ClientProtocolException("method requested not defined: " + httpMethod);
		
		return doRequest(req);
	}
	

	/*
	 * assembles the request for POSTs and PUTs (uses a different base class for these entity-enclosing methods)
	 */
	private HttpResponse doRequestMMBody(String url,String httpMethod, SimpleMultipartEntity mpe) throws ClientProtocolException, IOException {
		log.info("restURL: " + url);
		log.info("method: " + httpMethod);
		
		HttpEntityEnclosingRequestBase req = null;
		if (httpMethod == Constants.PUT)
			req = new HttpPut(url);
		else if (httpMethod == Constants.POST)
			req = new HttpPost(url);
		else 
			throw new ClientProtocolException("method requested not defined: " + httpMethod);
	
		if (mpe != null) {
			req.setEntity(mpe);
			log.info("entity: present");
		} else {
			log.info("entity: null");
		}
		HttpResponse response = doRequest(req);
		if (mpe != null)
			mpe.cleanupTempFiles();
		return response;
	}

	/*
	 * applies the header settings and executes the request
	 */
	private HttpResponse doRequest(HttpUriRequest req) throws ClientProtocolException, IOException {

		for (String n: headers.keySet())
		{
			req.setHeader(n,(String)headers.get(n));
		}
		
		return httpClient.execute(req);
	}
}
