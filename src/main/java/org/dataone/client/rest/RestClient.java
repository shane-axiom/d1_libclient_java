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

package org.dataone.client.rest;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.dataone.mimemultipart.SimpleMultipartEntity;
import org.dataone.service.util.Constants;



/**
 * A generic client class that contains base functionality for making REST calls
 * to remote REST servers.
 * It is built to encapsulate the communication conventions dataONE is following
 * but does not know about dataone objects (see HttpMultipartRestClient for that).
 * Specifically, it requires encoding of message bodies as mime-multipart.
 *
 * The HttpClient can be set either in the constructor or via the setter.  It is
 * generally advised to not change the HttpClient once set.
 *
 * */
public class RestClient {

    protected static Log log = LogFactory.getLog(RestClient.class);


    protected HttpClient httpClient;
    protected HashMap<String, String> headers = new HashMap<String, String>();

    private Map<Long,String> perThreadLatestRequestUrlMap = new HashMap<Long,String>();

    /**
     * Default constructor to create a new instance.
     */


    public RestClient(HttpClient client)
    {
        this.httpClient = client;
    }

    /**
     * returns a description of the latest rest call made by this instance.
     * In order to handle multi-threaded uses of a single RestClient, the latest
     * call is tracked per thread, and will return the description for the latest
     * call on that thread.
     * @return
     */
    public String getLatestRequestUrl() {
        long threadId = Thread.currentThread().getId();
        return this.perThreadLatestRequestUrlMap.get(threadId);
    }

    private void setLatestRequestUrl(String value) {
        long threadId = Thread.currentThread().getId();
        this.perThreadLatestRequestUrlMap.put(threadId, value);
    }
    
    /**
     * To support cross thread retrieval of request info, a parameterized
     * form of this method.
     * @param t
     * @return
     */
    public String getLatestRequestUrl(Thread t) {
        long threadId = t.getId();
        return this.perThreadLatestRequestUrlMap.get(threadId);
    }


    /**
     * Gets the DefaultHttpClient instance used to make the connection
     * @return
     */
    public HttpClient getHttpClient()
    {
        return this.httpClient;
    }


    /**
     * Sets the HttpClient instance used for the connection.
     * Use with caution.
     * @param httpClient
     */
    public void setHttpClient(HttpClient httpClient)
    {
        this.httpClient = httpClient;
    }





    /**
     * adds or replaces a header with the provided key and value to the RestClient
     * instance to be used for all subsequent http requests (added to each HttpRequest). 
     * Not thread-safe, so will affect all future request invocations.
     * 
     * @deprecated As of release 2.1, due to thread safety issues. Use an HttpRequestInterceptor 
     * added to the HttpClient build time, as shown in https://redmine.dataone.org/issues/7638
     * @param name
     * @param value
     */
    @Deprecated
    public void setHeader(String name, String value)
    {
        headers.put(name, value);
    }

    /**
     * returns a Map of the headers added via setHeader(..)
     * @return
     * @deprecated As of release 2.1, due to thread safety issues. Use an HttpRequestInterceptor 
     * added to the HttpClient build time, as shown in https://redmine.dataone.org/issues/7638
     */
    @Deprecated
    public HashMap<String, String> getAddedHeaders()
    {
        return headers;
    }

    /**
     * clears the map of added headers.  Not thread-aware, so will affect all 
     * request invocations.
     * @deprecated As of release 2.1, due to thread safety issues. Use an HttpRequestInterceptor 
     * added to the HttpClient build time, as shown in https://redmine.dataone.org/issues/7638
     * @return
     */
    @Deprecated
    public void clearAddedHeaders()
    {
        headers.clear();
    }


    /**
     * send a GET request to the resource and get the response
     * @throws IOException
     * @throws ClientProtocolException
     */
    public HttpResponse doGetRequest(String url, RequestConfig requestConfig)
            throws ClientProtocolException, IOException {
        return doRequestNoBody(url,Constants.GET,requestConfig);
    }

    /**
     * send a Head request to the resource and get the response
     * @throws IOException
     * @throws ClientProtocolException
     */

    public HttpResponse doHeadRequest(String url, RequestConfig requestConfig)
            throws ClientProtocolException, IOException  {
        return doRequestNoBody(url,Constants.HEAD,requestConfig);
    }

    /**
     * send a Delete request to the resource and get the response
     * @throws IOException
     * @throws ClientProtocolException
     */

    public HttpResponse doDeleteRequest(String url, RequestConfig requestConfig)
            throws ClientProtocolException, IOException {
        return doRequestNoBody(url,Constants.DELETE,requestConfig);
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
    public HttpResponse doPostRequest(String url, SimpleMultipartEntity mpe, RequestConfig requestConfig)
            throws ClientProtocolException, IOException {
        return doRequestMMBody(url,Constants.POST,mpe,requestConfig);
    }

    /**
     * send a PUT request to the resource and get the response
     * @throws IOException
     * @throws ClientProtocolException
     */
    public HttpResponse doPutRequest(String url, SimpleMultipartEntity mpe, RequestConfig requestConfig)
            throws ClientProtocolException, IOException {
        return doRequestMMBody(url,Constants.PUT,mpe,requestConfig);
    }


    /*
     * assembles the request for GETs, HEADs and DELETEs - assumes no message body
     */
    private HttpResponse doRequestNoBody(String url,String httpMethod, RequestConfig requestConfig)
            throws ClientProtocolException, IOException {

        String latestCall = httpMethod + " " + url;

        HttpResponse response = null;
        try {
            HttpRequestBase req = null;
            if (httpMethod == Constants.GET)
                req = new HttpGet(url);

            else if (httpMethod == Constants.HEAD)
                req = new HttpHead(url);

            else if (httpMethod == Constants.DELETE)
                req = new HttpDelete(url);

            else
                throw new ClientProtocolException("method requested not defined: " + httpMethod);

            req.setConfig(requestConfig);
            response = doRequest(req);
        }
        finally {
            setLatestRequestUrl(latestCall);
            log.info("RestClient.doRequestNoBody, thread(" + Thread.currentThread().getId() + ") call Info: " + latestCall);
        }
        return response;
    }


    /*
     * assembles the request for POSTs and PUTs (uses a different base class for these entity-enclosing methods)
     */
    private HttpResponse doRequestMMBody(String url,String httpMethod, SimpleMultipartEntity mpe, RequestConfig requestConfig)
            throws ClientProtocolException, IOException {
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
            }
            else {
                latestCall += "; MMP entity is null";
            }

            // HttpClient (v4.3.x) uses a default RequestConfig and allows an
            // overriding one to be passed in on the request.
            if (requestConfig != null)
                req.setConfig(requestConfig);

            response = doRequest(req);
        }
        finally {
            setLatestRequestUrl(latestCall);
            log.info("RestClient.doRequestMMPBody, thread(" + Thread.currentThread().getId() + ") call Info: " + latestCall);
//            log.info("rest call info. threadId(" + Thread.currentThread().getId() + ") " + latestCall);
            
            if (mpe != null)
                if (! mpe.cleanupTempFiles() ) {
                    log.warn("Failed to clean up temp files for: " + httpMethod + " " + url);
                }
        }
        return response;
    }

    /*
     * applies the headers to the request and executes the request
     */
    private HttpResponse doRequest(HttpUriRequest req)
            throws ClientProtocolException, IOException {
        for (String n: headers.keySet())
        {
            req.setHeader(n,(String)headers.get(n));
        }
        return httpClient.execute(req);
    }
}
