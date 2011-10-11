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
import java.io.InputStream;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.dataone.client.auth.CertificateManager;
import org.dataone.mimemultipart.SimpleMultipartEntity;
import org.dataone.service.exceptions.AuthenticationTimeout;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidCredentials;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidSystemMetadata;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.SynchronizationFailed;
import org.dataone.service.exceptions.UnsupportedMetadataType;
import org.dataone.service.exceptions.UnsupportedQueryType;
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.types.v1.Session;
import org.dataone.service.util.ExceptionHandler;

/**
 * This class wraps the RestClient, adding uniform exception deserialization and
 * the setup of SSL for standard DataONE communications.
 * 
 * 
 */
public class D1RestClient {
	
	protected static Log log = LogFactory.getLog(D1RestClient.class);
	
    protected RestClient rc;

	/**
	 * Default constructor to create a new instance.  SSL is setup using
	 * the default location for the client certificate.
	 */
	public D1RestClient() {
		this.rc = new RestClient();
		setupSSL(null);
	}
	
	/**
	 * Constructor to create a new instance with given session/subject.
	 * The session's subject is used to find the registered certificate and key
	 * 
	 */
	public D1RestClient(Session session) {
		this.rc = new RestClient();
		setupSSL(session);
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
		this.rc.setTimeouts(milliseconds);
	}
 
	
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
		SSLSocketFactory socketFactory = null;
		try {
			socketFactory = CertificateManager.getInstance().getSSLSocketFactory(session);
		} catch (FileNotFoundException e) {
			// these are somewhat expected for anonymous d1 client use
			log.warn("Could not set up SSL connection for client - likely because the certificate could not be located: " + e.getMessage());
		} catch (Exception e) {
			// this is likely more severe
			log.warn("Funky SSL going on: " + e.getClass() + ":: " + e.getMessage());
		}
		try {
			//443 is the default port, this value is overridden if explicitly set in the URL
			Scheme sch = new Scheme("https", 443, socketFactory );
			this.rc.getHttpClient().getConnectionManager().getSchemeRegistry().register(sch);
		} catch (Exception e) {
			// this is likely more severe
			log.error("Failed to set up SSL connection for client. Continuing. " + e.getClass() + ":: " + e.getMessage(), e);
		}
	}
	
	
	/**
	 * Perform an HTTP GET request, setting the headers first and parsing /filtering
	 * exceptions to the exception stream on the response into their
	 * respective java instances.
	 * 
	 * @param url - the encoded url string
	 * @return the InputStream from the http Response
	 * 
	 * @throws AuthenticationTimeout
	 * @throws IdentifierNotUnique
	 * @throws InsufficientResources
	 * @throws InvalidCredentials
	 * @throws InvalidRequest
	 * @throws InvalidSystemMetadata
	 * @throws InvalidToken
	 * @throws NotAuthorized
	 * @throws NotFound
	 * @throws NotImplemented
	 * @throws ServiceFailure
	 * @throws SynchronizationFailed
	 * @throws UnsupportedMetadataType
	 * @throws UnsupportedQueryType
	 * @throws UnsupportedType
	 * @throws IllegalStateException
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws HttpException
	 */
	public InputStream doGetRequest(String url) 
	throws AuthenticationTimeout, IdentifierNotUnique, InsufficientResources, 
	InvalidCredentials, InvalidRequest, InvalidSystemMetadata, InvalidToken, 
	NotAuthorized, NotFound, NotImplemented, ServiceFailure, SynchronizationFailed,
	UnsupportedMetadataType, UnsupportedQueryType, UnsupportedType,
	IllegalStateException, ClientProtocolException, IOException, HttpException 
	{
		rc.setHeader("Accept", "text/xml");
		return ExceptionHandler.filterErrors(rc.doGetRequest(url));
	}

	
	public InputStream doDeleteRequest(String url) 
	throws AuthenticationTimeout, IdentifierNotUnique, InsufficientResources, 
	InvalidCredentials, InvalidRequest, InvalidSystemMetadata, InvalidToken, 
	NotAuthorized, NotFound, NotImplemented, ServiceFailure, SynchronizationFailed,
	UnsupportedMetadataType, UnsupportedQueryType, UnsupportedType,
	IllegalStateException, ClientProtocolException, IOException, HttpException 
	{
		rc.setHeader("Accept", "text/xml");
		return ExceptionHandler.filterErrors(rc.doDeleteRequest(url));
	}
	
	public InputStream doDeleteRequest(String url, SimpleMultipartEntity mpe) 
	throws AuthenticationTimeout, IdentifierNotUnique, InsufficientResources, 
	InvalidCredentials, InvalidRequest, InvalidSystemMetadata, InvalidToken, 
	NotAuthorized, NotFound, NotImplemented, ServiceFailure, SynchronizationFailed,
	UnsupportedMetadataType, UnsupportedQueryType, UnsupportedType,
	IllegalStateException, ClientProtocolException, IOException, HttpException 
	{
		rc.setHeader("Accept", "text/xml");
		return ExceptionHandler.filterErrors(rc.doDeleteRequest(url, mpe));
	}
	
	public Header[] doHeadRequest(String url) 
	throws AuthenticationTimeout, IdentifierNotUnique, InsufficientResources, 
	InvalidCredentials, InvalidRequest, InvalidSystemMetadata, InvalidToken, 
	NotAuthorized, NotFound, NotImplemented, ServiceFailure, SynchronizationFailed,
	UnsupportedMetadataType, UnsupportedQueryType, UnsupportedType,
	IllegalStateException, ClientProtocolException, IOException, HttpException 
	{
		rc.setHeader("Accept", "text/xml");
		return ExceptionHandler.filterErrorsHeader(rc.doHeadRequest(url));
	}
	
	public InputStream doPutRequest(String url, SimpleMultipartEntity entity) 
	throws AuthenticationTimeout, IdentifierNotUnique, InsufficientResources, 
	InvalidCredentials, InvalidRequest, InvalidSystemMetadata, InvalidToken, 
	NotAuthorized, NotFound, NotImplemented, ServiceFailure, SynchronizationFailed,
	UnsupportedMetadataType, UnsupportedQueryType, UnsupportedType,
	IllegalStateException, ClientProtocolException, IOException, HttpException 
	{
		rc.setHeader("Accept", "text/xml");
		return ExceptionHandler.filterErrors(rc.doPutRequest(url, entity));
	}
	
	public InputStream doPostRequest(String url, SimpleMultipartEntity entity) 
	throws AuthenticationTimeout, IdentifierNotUnique, InsufficientResources, 
	InvalidCredentials, InvalidRequest, InvalidSystemMetadata, InvalidToken, 
	NotAuthorized, NotFound, NotImplemented, ServiceFailure, SynchronizationFailed,
	UnsupportedMetadataType, UnsupportedQueryType, UnsupportedType,
	IllegalStateException, ClientProtocolException, IOException, HttpException 
	{
		rc.setHeader("Accept", "text/xml");
		return ExceptionHandler.filterErrors(rc.doPostRequest(url,entity));
	}
	

	public void setHeader(String name, String value) {
		rc.setHeader(name, value);
	}
	
	public HashMap<String, String> getAddedHeaders() {
		return rc.getAddedHeaders();
	}
}
