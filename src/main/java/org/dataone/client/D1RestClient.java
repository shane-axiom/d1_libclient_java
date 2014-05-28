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
import java.io.InputStream;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.AbstractHttpClient;
import org.dataone.client.auth.CertificateManager;
import org.dataone.client.exception.ClientSideException;
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
public class D1RestClient implements MultipartRestClient {
	
	protected static Log log = LogFactory.getLog(D1RestClient.class);
	
    protected RestClient rc;

	/**
	 * Default constructor to create a new instance.  SSL is setup using
	 * the default location for the client certificate.
	 */
//	public D1RestClient() {
//		this.rc = new DefaultRestClient();
//		setupSSL(null);
//	}
	
	
	public D1RestClient(HttpClient httpClient) {
		this.rc = new RestClient(httpClient);
		setupSSL(null);
	}
	
	
	/**
	 * Constructor to create a new instance with given session/subject.
	 * The session's subject is used to find the registered certificate and key
	 * 
	 */
//	public D1RestClient(Session session) {
//		this.rc = new DefaultRestClient();
//		setupSSL(session);
//	}

	
	public D1RestClient(HttpClient httpClient, Session session) {
		this.rc = new RestClient(httpClient);
		setupSSL(session);
	}

	/**
	 * Gets the AbstractHttpClient instance used to make the connection
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
	public void closeIdleConnections()
	{
		getHttpClient().getConnectionManager().closeIdleConnections(0,TimeUnit.MILLISECONDS);
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
//	public void setTimeouts(int milliseconds) {
//		((AbstractHttpClient) this.getHttpClient()).setTimeouts(milliseconds);
//	}
 
	
	/**
	 * Method used by the constructors to setup the connection with SSL.
	 * With the current CertificateManager implementation, a null value for 
	 * the session object causes the certificate at the default location to 
	 * be used.  If the certificate is not found, a warning will be logged 
	 * and SSL scheme setup will continue.
	 * <p>
	 * Calling from a D1RestClient instance should override previous
	 * connection setups
	 *  
	 * @param session
	 */
	public void setupSSL(Session session) 
	{		
//		long startMS = new Date().getTime();
		SSLSocketFactory socketFactory = null;
		try {
			String subjectString = null;
			if (session != null && session.getSubject() != null) {
				subjectString = session.getSubject().getValue();
			}
			socketFactory = CertificateManager.getInstance().getSSLSocketFactory(subjectString);
		} catch (Exception e) {
			// this is likely more severe
			log.warn("Exception from CertificateManager at SSL setup - client will be anonymous: " + 
					e.getClass() + ":: " + e.getMessage());
		}
		try {
			//443 is the default port, this value is overridden if explicitly set in the URL
			Scheme sch = new Scheme("https", 443, socketFactory );
			this.rc.getHttpClient().getConnectionManager().getSchemeRegistry().register(sch);
		} catch (Exception e) {
			// this is likely more severe
			log.error("Failed to set up SSL connection for client. Continuing. " + e.getClass() + ":: " + e.getMessage(), e);
		}
//		long deltaT = new Date().getTime() - startMS;
//		log.warn("  SSLsetupTime: " + deltaT);
	}
	
	
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
			return ExceptionHandler.filterErrors(rc.doGetRequest(url), allowRedirect);
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
			return ExceptionHandler.filterErrorsHeader(rc.doGetRequest(url),Constants.GET);
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
			return ExceptionHandler.filterErrors(rc.doDeleteRequest(url));
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
			return ExceptionHandler.filterErrorsHeader(rc.doHeadRequest(url),Constants.HEAD);
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
			return ExceptionHandler.filterErrors(rc.doPutRequest(url, entity));
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
			return ExceptionHandler.filterErrors(rc.doPostRequest(url,entity));
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
