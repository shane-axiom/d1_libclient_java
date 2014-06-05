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

package org.dataone.client.impl.rest;

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
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.AbstractHttpClient;
import org.dataone.client.auth.CertificateManager;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.rest.MultipartRestClient;
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
 * 
 */
public class HttpMultipartRestClient implements MultipartRestClient {
	
	protected static Log log = LogFactory.getLog(HttpMultipartRestClient.class);
	
    protected RestClient rc;
    protected RequestConfig reqConfig;

	/**
	 * Default constructor to create a new instance.  SSL is setup using
	 * the default location for the client certificate.
	 */
	public HttpMultipartRestClient(HttpClient httpClient, RequestConfig reqConfig) {
		this.rc = new RestClient(httpClient);
		this.reqConfig = reqConfig;
		HttpUtils.setupSSL(httpClient, null);
	}
	
	
	/**
	 * Constructor to create a new instance with given session/subject.
	 * The session's subject is used to find the registered certificate and key
	 * 
	 */	
	public HttpMultipartRestClient(HttpClient httpClient, Session session, RequestConfig reqConfig) {
		this.rc = new RestClient(httpClient);
		this.reqConfig = reqConfig;
		HttpUtils.setupSSL(httpClient, session);
	}

	public void setRequestConfig(RequestConfig reqConfig) {
		this.reqConfig = reqConfig;
	}
	
	public RequestConfig getRequestConfig() {
		return this.reqConfig;
	}
	
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
	public InputStream doGetRequest(String url) 
	throws BaseException, ClientSideException
	{
		return doGetRequest(url,false);
	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.MultipartRestClient#doGetRequest(java.lang.String, boolean)
	 */
	@Override
	public InputStream doGetRequest(String url, boolean allowRedirect) 
			throws BaseException, ClientSideException
	{
		rc.setHeader("Accept", "text/xml");
		try {
			return ExceptionHandler.filterErrors(rc.doGetRequest(url, this.reqConfig), allowRedirect);
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
	public Header[] doGetRequestForHeaders(String url) 
			throws BaseException, ClientSideException
	{
		rc.setHeader("Accept", "text/xml");
		try {
			return ExceptionHandler.filterErrorsHeader(rc.doGetRequest(url, this.reqConfig),Constants.GET);
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
	public InputStream doDeleteRequest(String url) 
			throws BaseException, ClientSideException
	{
		rc.setHeader("Accept", "text/xml");
		try {
			return ExceptionHandler.filterErrors(rc.doDeleteRequest(url, this.reqConfig));
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
//		rc.setHeader("Accept", "text/xml");
//		return ExceptionHandler.filterErrors(rc.doDeleteRequest(url, mpe));
//	}
	
	/* (non-Javadoc)
	 * @see org.dataone.client.MultipartRestClient#doHeadRequest(java.lang.String)
	 */
	@Override
	public Header[] doHeadRequest(String url) 
			throws BaseException, ClientSideException
	{
		rc.setHeader("Accept", "text/xml");
		try {
			return ExceptionHandler.filterErrorsHeader(rc.doHeadRequest(url, this.reqConfig),Constants.HEAD);
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
	public InputStream doPutRequest(String url, SimpleMultipartEntity entity) 
			throws BaseException, ClientSideException
	{
		rc.setHeader("Accept", "text/xml");
		try {
			return ExceptionHandler.filterErrors(rc.doPutRequest(url, entity, this.reqConfig));
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
	public InputStream doPostRequest(String url, SimpleMultipartEntity entity) 
			throws BaseException, ClientSideException
	{
		rc.setHeader("Accept", "text/xml");
		try {
			return ExceptionHandler.filterErrors(rc.doPostRequest(url,entity, this.reqConfig));
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
}
