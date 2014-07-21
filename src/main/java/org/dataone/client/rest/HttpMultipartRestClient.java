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
import java.io.InputStream;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.DefaultHttpClient;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.utils.HttpUtils;
import org.dataone.mimemultipart.SimpleMultipartEntity;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.types.v1.Session;
import org.dataone.service.util.Constants;
import org.dataone.service.util.ExceptionHandler;

/**
 * This class wraps the RestClient, adding uniform exception deserialization and
 * the setup of SSL for standard DataONE communications.
 * 
 * Timeouts are converted into RequestConfigs in this class, and produced by copying
 * the existing default one and setting connection request, connection, and socket timeouts
 * with the supplied value.
 * 
 */
public class HttpMultipartRestClient implements MultipartRestClient {
	
	protected static Log log = LogFactory.getLog(HttpMultipartRestClient.class);
	
    protected RestClient rc;
    
    protected RequestConfig baseRequestConfig;

	/**
	 * Default constructor to create a new instance.  SSL is setup using
	 * the default location for the client certificate.
	 */
	public HttpMultipartRestClient(HttpClient httpClient) {
		this(httpClient, null, null);
	}
	
	
	/**
	 * An alternate constructor for special-case situations.  See explanation
	 * in parameter descriptions.  
	 * 
	 * @param httpClient 
	 * @param requestConfig - set this parameter with the custom ReqeustConfig used
	 * to build the HttpClient (v4.3.x or later), so per-request configurations can
	 * use those configurations while at the same time overriding timeout ones. 
	 * @param session - if using a v4.1.x httpClient (DefaultHttpClient), providing a
	 * Session object sets up the SSL using the certificate associated with that session's subject.
	 * 
	 */
	public HttpMultipartRestClient(HttpClient httpClient, RequestConfig requestConfig, Session session) {
		this.rc = new RestClient(httpClient);
		this.baseRequestConfig = requestConfig;
		if (httpClient instanceof DefaultHttpClient)
			HttpUtils.setupSSL_v4_1((DefaultHttpClient)httpClient, session);
	}

//	public void setRequestConfig(RequestConfig reqConfig) {
//		this.reqConfig = reqConfig;
//	}
//	
//	public RequestConfig getRequestConfig() {
//		return this.reqConfig;
//	}
	
	/**
	 * Gets the HttpClient instance used to make the connection
	 * @return
	 */
	public HttpClient getHttpClient() 
	{
		return this.rc.getHttpClient();
	}

	/**
	 * Gets the string representation of the latest http call made by the 
	 * underlying RestClient
	 * @return
	 */
	public String getLatestRequestUrl() 
	{
		return rc.getLatestRequestUrl();
	}
	
	
	/**
	 * Calls closeIdleConnections on the underlying connection manager. This will
	 * effectively close all released connections managed by the connection manager.
	 */
	@Deprecated
	public void closeIdleConnections()
	{
		getHttpClient().getConnectionManager().closeIdleConnections(0,TimeUnit.MILLISECONDS);
	}
	

//	/**
//	 * Sets the CONNECTION_TIMEOUT and SO_TIMEOUT values for the underlying httpClient.
//	 * (max delay in initial response, max delay between tcp packets, respectively).  
//	 * Uses the same value for both.
//	 * 
//	 * (The default value set in the constructor is 30 seconds)
//	 * 
//	 * @param milliseconds
//	 */
//	@Deprecated
//	public void setTimeouts(int milliseconds) {
//		((AbstractHttpClient) this.getHttpClient()).setTimeouts(milliseconds);
//	}
 
	

	
	
	/* (non-Javadoc)
	 * @see org.dataone.client.MultipartRestClient#doGetRequest(java.lang.String)
	 */
	@Override
	public InputStream doGetRequest(String url, Integer timeoutMillisecs) 
	throws BaseException, ClientSideException
	{
		return doGetRequest(url, timeoutMillisecs, false);
	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.MultipartRestClient#doGetRequest(java.lang.String, boolean)
	 */
	@Override
	public InputStream doGetRequest(String url, Integer timeoutMillisecs, boolean allowRedirect) 
			throws BaseException, ClientSideException
	{
		try {
			return ExceptionHandler.filterErrors( 
					rc.doGetRequest(url,determineTimeoutConfig(timeoutMillisecs)), 
					allowRedirect);
		} catch (IllegalStateException e) {
			throw new ClientSideException("", e);
		} catch (ClientProtocolException e) {
			throw new ClientSideException("", e);
		} catch (IOException e) {
			throw new ClientSideException("", e);
		} catch (HttpException e) {
			throw new ClientSideException("", e);
		}
	}

	/* (non-Javadoc)
	 * @see org.dataone.client.MultipartRestClient#doGetRequestForHeaders(java.lang.String)
	 */
	@Override
	public Header[] doGetRequestForHeaders(String url, Integer timeoutMillisecs) 
			throws BaseException, ClientSideException
	{
		determineTimeoutConfig(timeoutMillisecs);
		try {
			return ExceptionHandler.filterErrorsHeader( 
					rc.doGetRequest(url,determineTimeoutConfig(timeoutMillisecs)), 
					Constants.GET);
		} catch (IllegalStateException e) {
			throw new ClientSideException("", e);
		} catch (ClientProtocolException e) {
			throw new ClientSideException("", e);
		} catch (IOException e) {
			throw new ClientSideException("", e);
		} catch (HttpException e) {
			throw new ClientSideException("", e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.MultipartRestClient#doDeleteRequest(java.lang.String)
	 */
	@Override
	public InputStream doDeleteRequest(String url, Integer timeoutMillisecs) 
			throws BaseException, ClientSideException
	{
		determineTimeoutConfig(timeoutMillisecs);
		try {
			return ExceptionHandler.filterErrors( 
					rc.doDeleteRequest(url, determineTimeoutConfig(timeoutMillisecs)) );
		} catch (IllegalStateException e) {
			throw new ClientSideException("", e);
		} catch (ClientProtocolException e) {
			throw new ClientSideException("", e);
		} catch (IOException e) {
			throw new ClientSideException("", e);
		} catch (HttpException e) {
			throw new ClientSideException("", e);
		}
	}
	
//	public InputStream doDeleteRequest(String url, SimpleMultipartEntity mpe) 
//	throws AuthenticationTimeout, IdentifierNotUnique, InsufficientResources, 
//	InvalidCredentials, InvalidRequest, InvalidSystemMetadata, InvalidToken, 
//	NotAuthorized, NotFound, NotImplemented, ServiceFailure, SynchronizationFailed,
//	UnsupportedMetadataType, UnsupportedQueryType, UnsupportedType,
//	IllegalStateException, ClientProtocolException, IOException, HttpException, VersionMismatch 
//	{
//		return ExceptionHandler.filterErrors(rc.doDeleteRequest(url, mpe));
//	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.MultipartRestClient#doHeadRequest(java.lang.String)
	 */
	@Override
	public Header[] doHeadRequest(String url, Integer timeoutMillisecs) 
			throws BaseException, ClientSideException
	{
		try {
			return ExceptionHandler.filterErrorsHeader( 
					rc.doHeadRequest(url,determineTimeoutConfig(timeoutMillisecs)), 
					Constants.HEAD);
		} catch (IllegalStateException e) {
			throw new ClientSideException("", e);
		} catch (ClientProtocolException e) {
			throw new ClientSideException("", e);
		} catch (IOException e) {
			throw new ClientSideException("", e);
		} catch (HttpException e) {
			throw new ClientSideException("", e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.MultipartRestClient#doPutRequest(java.lang.String, org.dataone.mimemultipart.SimpleMultipartEntity)
	 */
	@Override
	public InputStream doPutRequest(String url, SimpleMultipartEntity entity, Integer timeoutMillisecs) 
			throws BaseException, ClientSideException
	{
		determineTimeoutConfig(timeoutMillisecs);
		try {
			return ExceptionHandler.filterErrors( 
					rc.doPutRequest(url,entity,determineTimeoutConfig(timeoutMillisecs)) );
		} catch (IllegalStateException e) {
			throw new ClientSideException("", e);
		} catch (ClientProtocolException e) {
			throw new ClientSideException("", e);
		} catch (IOException e) {
			throw new ClientSideException("", e);
		} catch (HttpException e) {
			throw new ClientSideException("", e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.MultipartRestClient#doPostRequest(java.lang.String, org.dataone.mimemultipart.SimpleMultipartEntity)
	 */
	@Override
	public InputStream doPostRequest(String url, SimpleMultipartEntity entity, Integer timeoutMillisecs) 
			throws BaseException, ClientSideException
	{
		determineTimeoutConfig(timeoutMillisecs);
		try {
			return ExceptionHandler.filterErrors( 
					rc.doPostRequest(url,entity,determineTimeoutConfig(timeoutMillisecs)) );
		} catch (IllegalStateException e) {
			throw new ClientSideException("", e);
		} catch (ClientProtocolException e) {
			throw new ClientSideException("", e);
		} catch (IOException e) {
			throw new ClientSideException("", e);
		} catch (HttpException e) {
			throw new ClientSideException("", e);
		}
	}
	

	public void setHeader(String name, String value) {
		rc.setHeader(name, value);
	}
	
	public HashMap<String, String> getAddedHeaders() {
		return rc.getAddedHeaders();
	}
	
	
	/*
	 * function to either build a RequestConfig based on a base RequestConfig
	 * passed in on the constructor, or one from scratch.
	 */
	private RequestConfig determineTimeoutConfig(Integer milliseconds) 
	{     
		RequestConfig config= null;
		
		if (milliseconds != null) {
			RequestConfig.Builder rcBuilder = null;
			if (this.baseRequestConfig != null) {
				rcBuilder = RequestConfig.copy(this.baseRequestConfig);
			} else {
				rcBuilder = RequestConfig.custom();
			}
			config = rcBuilder
        		.setConnectTimeout(milliseconds)
        		.setConnectionRequestTimeout(milliseconds)
        		.setSocketTimeout(milliseconds)
        		.build();
		}
		return config;
	}
	
        	
// for httpClient v4.1.x 
//	private void setTimeoutConfig(Integer milliseconds) 
//	{  
//        	HttpClient hc = this.getHttpClient();
//        
//        	HttpParams params = hc.getParams();
//        	// the timeout in milliseconds until a connection is established.
//        	HttpConnectionParams.setConnectionTimeout(params, timeout);
//        
//        	//defines the socket timeout (SO_TIMEOUT) in milliseconds, which is the timeout
//        	// for waiting for data or, put differently, a maximum period inactivity between
//        	// two consecutive data packets).
//        	HttpConnectionParams.setSoTimeout(params, timeout);
//  	
//        	this.setParams(params);
//	}
}
