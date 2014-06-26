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


import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.AbstractHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.dataone.mimemultipart.SimpleMultipartEntity;
import org.dataone.service.util.Constants;



/**
 * A generic client class that contains base functionality for making REST calls
 * to remote REST servers.  
 * It is built to encapsulate the communication conventions dataONE is following
 * but does not know about dataone objects (see D1RestClient for that).  
 * Specifically, it requires encoding of message bodies as mime-multipart.
 * It exposes the underlying httpClient for further configuration of the 
 * connection.
 * 
 * ( Each RestClient has an HttpClient that is reused for all do{XX}Requests )
 * 
 * */
public class RestClient {

	protected static Log log = LogFactory.getLog(RestClient.class);
	
	
    private AbstractHttpClient httpClient;
    private HashMap<String, String> headers = new HashMap<String, String>();
    
    private String latestRequestUrl = null;
    
	/**
	 * Default constructor to create a new instance.
	 */
	public RestClient() 
	{
		httpClient = new DefaultHttpClient();
	    setTimeouts(30 * 1000);
	}
	
	public RestClient(AbstractHttpClient client)
	{
		httpClient = client;
		setTimeouts(30 * 1000);
	}
	
	public String getLatestRequestUrl() {
		return latestRequestUrl;
	}
	
	private void setLatestRequestUrl(String value) {
		latestRequestUrl = value;
	}
	
	/**
	 * Gets the DefaultHttpClient instance used to make the connection
	 * @return
	 */
	public HttpClient getHttpClient() 
	{
		return httpClient;
	}
	
	
	/**
	 * Sets the AbstractHttpClient instance used for the connection.
	 * Use with caution.  AbstractHttpClient is necessary to make use of
	 * the setParams() method.
	 * @param httpClient
	 */
	public void setHttpClient(AbstractHttpClient httpClient) 
	{
		this.httpClient = httpClient;
	}
	
	
	/**
	 * Sets the CONNECTION_TIMEOUT and SO_TIMEOUT values for the underlying httpClient.
	 * (max delay in initial response, max delay between tcp packets, respectively).  
	 * Uses the same value for both.
	 * 
	 * (The default value set by the constructor is 30 seconds)
	 * 
	 * @param milliseconds
	 */
	public void setTimeouts(int milliseconds) 
	{
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

	
	/**
	 * adds headers with the provided key and value to the RestClient instance
	 * to be used for subsequent calls
	 * @param name
	 * @param value
	 */
	public void setHeader(String name, String value) 
	{
		headers.put(name, value);
	}
	
	/**
	 * returns a Map of the headers added via setHeader(..)
	 * @return
	 */
	public HashMap<String, String> getAddedHeaders() 
	{
		return headers;
	}
	
	/**
	 * clears the map of added headers
	 * @return
	 */
	public void clearAddedHeaders() 
	{
		headers.clear();
	}
	
	
    /**
	 * send a GET request to the resource and get the response
     * @throws IOException 
     * @throws ClientProtocolException  
	 */
	public HttpResponse doGetRequest(String url) 
	throws ClientProtocolException, IOException  
	{
		return doRequestNoBody(url,Constants.GET);
	}

    /**
	 * send a Head request to the resource and get the response
     * @throws IOException 
     * @throws ClientProtocolException 
	 */

	public HttpResponse doHeadRequest(String url) 
	throws ClientProtocolException, IOException  
	{
		return doRequestNoBody(url,Constants.HEAD);
	}

    /**
	 * send a Delete request to the resource and get the response
     * @throws IOException 
     * @throws ClientProtocolException 
	 */

	public HttpResponse doDeleteRequest(String url) 
	throws ClientProtocolException, IOException 
	{
		return doRequestNoBody(url,Constants.DELETE);
	}
	
//	/**
//	 * send a body-containing Delete request to the resource and get the response
//     * @throws IOException 
//     * @throws ClientProtocolException 
//	 */
//
//	public HttpResponse doDeleteRequest(String url, SimpleMultipartEntity mpe) 
//	throws ClientProtocolException, IOException 
//	{
//		return doRequestMMBody(url,Constants.DELETE, mpe);
//	}
	
    /**
	 * send a POST request to the resource and get the response
     * @throws IOException 
     * @throws ClientProtocolException  
	 */
	public HttpResponse doPostRequest(String url, SimpleMultipartEntity mpe) 
	throws ClientProtocolException, IOException
	{
		return doRequestMMBody(url,Constants.POST,mpe);
	}

    /**
	 * send a PUT request to the resource and get the response
     * @throws IOException 
     * @throws ClientProtocolException 
	 */
	public HttpResponse doPutRequest(String url, SimpleMultipartEntity mpe) 
	throws ClientProtocolException, IOException  
	{
		return doRequestMMBody(url,Constants.PUT,mpe);
	}
	

	/*
	 * assembles the request for GETs, HEADs and DELETEs - assumes no message body
	 */
	private synchronized HttpResponse doRequestNoBody(String url,String httpMethod) 
	throws ClientProtocolException, IOException
	{
		String latestCall = httpMethod + " " + url;
	
		HttpResponse response = null;
		try {
			HttpUriRequest req = null;
			if (httpMethod == Constants.GET) 
				req = new HttpGet(url);

			else if (httpMethod == Constants.HEAD) 
				req = new HttpHead(url);

			else if (httpMethod == Constants.DELETE) 
				req = new HttpDelete(url);

			else
				throw new ClientProtocolException("method requested not defined: " + httpMethod);
			
			response = doRequest(req);
		}
		finally {
			setLatestRequestUrl(latestCall);
			log.info("rest call info: " + latestCall);
		}
		return response;
	}
	

	/*
	 * assembles the request for POSTs and PUTs (uses a different base class for these entity-enclosing methods)
	 */
	private synchronized HttpResponse doRequestMMBody(String url,String httpMethod, SimpleMultipartEntity mpe)
	throws ClientProtocolException, IOException 
	{
		String latestCall = httpMethod + " " + url;
		
		HttpResponse response = null;
		try {
			HttpEntityEnclosingRequestBase req = null;
			if (httpMethod == Constants.PUT) 
				req = new HttpPut(url);
					
			else if (httpMethod == Constants.POST) 
				req = new HttpPost(url);

			else 
				throw new ClientProtocolException("method requested not defined: " + httpMethod);

			if (mpe != null) {
				req.setEntity(mpe);
				latestCall += "; MMP message has: " + mpe.getDescription();
			} else {
				latestCall += "; MMP entity is null";
			}
			response = doRequest(req);
		} 
		finally { 
			setLatestRequestUrl(latestCall);
			log.info("rest call info: " + latestCall);
			if (mpe != null)
				if (! mpe.cleanupTempFiles() ) {
					log.warn("failed to clean up temp files for: " + httpMethod + " " + url);
				}
		}
		return response;
	}

	/*
	 * applies the header settings and executes the request
	 */
	private HttpResponse doRequest(HttpUriRequest req) 
	throws ClientProtocolException, IOException
	{
		// Later versions of HttpClient check that 
		// the "host" parameter is set as a compliance to rfc2616, indicating 
		// that we need to set it.  However, shouldn't it happen for us?
		// TODO: double-check that it is needed.  commented out until then.
		
//		String host = req.getURI().getHost();
//		if (host == null) host = "";
//		req.setHeader("host", host);
		
		// set the other headers
		for (String n: headers.keySet())
		{
			req.setHeader(n,(String)headers.get(n));
		}
		
		
//		System.out.println("========== Request host parameter: " + host);
//		System.out.println("       " + req.getURI().toString());
		
		return httpClient.execute(req);
	}
}
