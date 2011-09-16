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
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.dataone.client.auth.CertificateManager;
import org.dataone.mimemultipart.SimpleMultipartEntity;
import org.dataone.service.util.Constants;



/**
 * A generic client class that contains base functionality for making REST calls
 * to remote REST servers.  
 * It is built to encapsulate the communication conventions dataONE is following
 * but does not know about dataone objects (see D1RestClient for that)
 * 
 * (Each RestClient instance has an HttpClient that is reused for the various
 * calls to the memberNode)
 * */
public class RestClient {

	protected static Log log = LogFactory.getLog(RestClient.class);
	
	
    private DefaultHttpClient httpClient;
    private HashMap<String, String> headers = new HashMap<String, String>();
    
	/**
	 * Constructor to create a new instance.
	 */
	public RestClient() {
	    httpClient = new DefaultHttpClient();
	    setTimeouts(30 * 1000); // seconds * 1000 = milliseconds  
	    setupSSL();
	}
	
	/**
	 * Sets the CONNECTION_TIMEOUT and SO_TIMEOUT values for the underlying httpClient.
	 * (max delay in initial response, max delay between tcp packets, respectively).  
	 * Uses the same value for both.
	 * 
	 * (The default value set in the constructor is 30 seconds)
	 * 
	 * @param milliseconds
	 */
	public void setTimeouts(int milliseconds) {

        Integer timeout = new Integer(milliseconds);
        

        HttpParams params = httpClient.getParams();
        // the timeout in milliseconds until a connection is established.
        HttpConnectionParams.setConnectionTimeout(params, timeout);
        
        //defines the socket timeout (SO_TIMEOUT) in milliseconds, which is the timeout
        // for waiting for data or, put differently, a maximum period inactivity between
        // two consecutive data packets).
        HttpConnectionParams.setSoTimeout(params, timeout);
      
        httpClient.setParams(params);
	}
	
	public void setupSSL() {
		
		SSLSocketFactory socketFactory = null;
		try {
			socketFactory = CertificateManager.getInstance().getSSLSocketFactory();
		} catch (FileNotFoundException e) {
			// these are somewhat expected for anonymous d1 client use
			log.warn("Could not set up SSL connection for client - likely because the certificate could not be located: " + e.getMessage());
		} catch (Exception e) {
			// this is likely more severe
			log.warn("Funky SSL going on: " + e.getMessage());
		}
		try {
			//443 is the default port, this value is overridden if explicitly set in the URL
			Scheme sch = new Scheme("https", 443, socketFactory );
			httpClient.getConnectionManager().getSchemeRegistry().register(sch);
		} catch (Exception e) {
			// this is likely more severe
			log.error("Failed to set up SSL connection for client: " + e.getMessage(), e);
		}
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
