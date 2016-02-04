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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.dataone.client.auth.AuthTokenSession;
import org.dataone.client.auth.CertificateManager;
import org.dataone.client.auth.X509Session;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.utils.HttpUtils;
import org.dataone.configuration.Settings;
import org.dataone.mimemultipart.SimpleMultipartEntity;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.util.Constants;
import org.dataone.service.util.ExceptionHandler;
import org.jibx.runtime.JiBXException;

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

    protected X509Session x509Session;
    
    public static final String DEFAULT_TIMEOUT_PARAM = "D1Client.default.timeout";
    
    public static final Integer DEFAULT_TIMEOUT_VALUE = 30000;
    
    /** 
     * HttpMRC requires a RestClient / HttpClient.
     * It optionally needs an X509Sesson property set to provide client-side
     * functionality (certificate checking). this can be null.
     * @throws ClientSideException 
     */
//    public HttpMultipartRestClient(X509Session x, RequestConfig requestConfig) 
//    throws ClientSideException {
//
//        if (x.getHttpClient() == null) {
//            try { 
//                if (requestConfig == null) {
//                    x.setHttpClient(HttpUtils.createHttpClient(x));
//                } else {
//                    x.setHttpClient(HttpUtils.getHttpClientBuilder(x)
//                            .setDefaultRequestConfig(requestConfig));
//                }
//            } catch (UnrecoverableKeyException | KeyManagementException
//                    | NoSuchAlgorithmException | KeyStoreException
//                    | CertificateException | InstantiationException
//                    | IllegalAccessException | IOException | JiBXException e) {
//                
//                throw new ClientSideException("Error building HttpClient", e);
//            } 
//        }
//        if (x.getHttpClient() instanceof HttpClient) {
//            this.rc = new RestClient((HttpClient)x.getHttpClient());
//        } else {
//            throw new ClientSideException("The type HttpMultipartRestClient only accepts HttpClients.");
//        }
//    }
    
    /** 
     * @param httpClient
     * @param requestConfig - if the httpClient is v4.3.x or later (not an
     * AbstractHttpClient), the requestConfiguration is set 
     */
//    public HttpMultipartRestClient(HttpClient httpClient, RequestConfig requestConfig) {
//        this.rc = new RestClient(httpClient);
//        
//        if (httpClient instanceof DefaultHttpClient) {
//            HttpUtils.setupSSL_v4_1((DefaultHttpClient)httpClient, null);
//            this.baseRequestConfig = null;  // it won't be used by the older HttpClient type
//        } else {
//            this.baseRequestConfig = requestConfig;
//        }
//    }
    
    /**
     * The basic constructor to implement the MultipartRestClient interface
     * using apache HttpClient and X509Certificate.  The user needs to ensure
     * that the X509Session (at least the Session and X509Certificate properties)
     * are the ones used when making the HttpClient, or the X509Session is null.
     * 
     * A null X509Session results in client-side certificate validity being skipped.
     * 
     * @param httpClient
     * @param x509Session
     */
    public HttpMultipartRestClient(HttpClient httpClient, X509Session x509session) {
        this.rc = new RestClient(httpClient);
        this.x509Session = x509session;
        setDefaultTimeout(DEFAULT_TIMEOUT_VALUE);
    }

    /**
     * This constructor is used to give users flexibility in creating the HttpClient,
     * but takes care of configuring the ConnectionManager with the X509Session
     * parameter. 
     * @param httpClientBuilder
     * @param x509session
     * @throws ClientSideException 
     */
    public HttpMultipartRestClient(HttpClientBuilder httpClientBuilder, 
            X509Session x509session) throws ClientSideException {
        
        HttpClientConnectionManager connMan = null;
        try {
            connMan = new PoolingHttpClientConnectionManager(
                    HttpUtils.buildConnectionRegistry(x509session));
        } catch (UnrecoverableKeyException | KeyManagementException
                | NoSuchAlgorithmException | KeyStoreException
                | CertificateException | IOException e) {
            
            throw new ClientSideException("Could not build the ConnectionRegistry", e);
        }
       
        this.rc = new RestClient(httpClientBuilder.setConnectionManager(connMan).build());
        this.x509Session = x509session;
        setDefaultTimeout(DEFAULT_TIMEOUT_VALUE);
    }

    
    
    /**
     * creates an HttpMultipartRestClient configured with the certificate in the
     * default or set location in the CertificateManager.
     * 
     * @throws IOException
     * @throws ClientSideException
     */
    public HttpMultipartRestClient() throws IOException, ClientSideException {
        this((String)null);
    }

    /**
     * creates an HttpMultipartRestClient configured with the certificate registered
     * to the CertificateManager.
     * 
     * @param subjectString
     * @throws FileNotFoundException 
     * @throws IOException
     * @throws ClientSideException
     */
    public HttpMultipartRestClient(String subjectString) throws 
    IOException, ClientSideException { 

        this(CertificateManager.getInstance().selectSession(subjectString));
    }
    
    /**
     * creates an HttpMultipartRestClient configured with the certificate 
     * contained in the X509Session.
     * 
     * @param x509Session
     * @throws IOException
     * @throws ClientSideException
     */
    public HttpMultipartRestClient(X509Session x509Session) throws  
    IOException, ClientSideException {
        try {
            this.rc = new RestClient(HttpUtils.createHttpClient(x509Session));
        } catch (UnrecoverableKeyException | KeyManagementException
                | NoSuchAlgorithmException | KeyStoreException
                | CertificateException | InstantiationException
                | IllegalAccessException | JiBXException e) {
            e.printStackTrace();
            throw new ClientSideException("Could not create HttpClient.", e);
        }
        this.x509Session = x509Session;
        setDefaultTimeout(DEFAULT_TIMEOUT_VALUE);
    }
    
    public HttpMultipartRestClient(AuthTokenSession authTokenSession) {
        this.rc = new RestClient(HttpUtils.createHttpClient(authTokenSession.getAuthToken()));
    }

    /**
     * An alternate constructor for older versions of HttpClient (v4.2.x and
     * earlier) that are based off ofAbstractHttpClient.  These HttpClients set 
     * up request configurations differently, and are generally associated with
     * earlier versions of ConnectionManagers that are deprecated. 
     *
     * @param httpClient
     * @param session - if using a v4.2.x httpClient, allows the session to configure
     * the connection manager held by the passed in httpClient. if an instance of
     * X509Session, will be used to check certificate validity.
     *
     */
//    public HttpMultipartRestClient(AbstractHttpClient abstractHttpClient, Session session) {
//        this.rc = new RestClient(abstractHttpClient);
//        this.baseRequestConfig = null;
//        if (session instanceof X509Session)
//            this.x509Cert = (X509Session)session;
//        
//        if (abstractHttpClient != null)
//            HttpUtils.setupSSL_v4_1(abstractHttpClient, session);
//    }
    

 
    
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
     * Gets the string representation of the current Thread's latest http request 
     * via this instance.
     * @return
     */
    public String getLatestRequestUrl()
    {
        return rc.getLatestRequestUrl();
    }
    
    /**
     * Gets the string representation of the specified Thread's latest http request 
     * via this instance.
     * @return
     */
    public String getLatestRequestUrl(Thread t)
    {
        return rc.getLatestRequestUrl(t);
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
            throws BaseException, ClientSideException {

        return doGetRequest(url, timeoutMillisecs, false);
    }

    /* (non-Javadoc)
     * @see org.dataone.client.MultipartRestClient#doGetRequest(java.lang.String, boolean)
     */
    @Override
    public InputStream doGetRequest(String url, Integer timeoutMillisecs, boolean followRedirect)
            throws BaseException, ClientSideException {
        
        try {
            HttpResponse response = rc.doGetRequest(url,determineRequestConfig(timeoutMillisecs, followRedirect));
            // if we're NOT following a redirect, we'll get an HTTP_SEE_OTHER (303)
            // and then we want filterErrors() to ALLOW that status code 
            // without throwing an exception, and vice-versa 
            return ExceptionHandler.filterErrors(response, !followRedirect);
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
            throws BaseException, ClientSideException {

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
            throws BaseException, ClientSideException {

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
            throws BaseException, ClientSideException {

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
            throws BaseException, ClientSideException {

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
            throws BaseException, ClientSideException {

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
        return determineRequestConfig(milliseconds, true);
    }
    
    /**
     * Returns a {@link RequestConfig} that is either based on the
     * existing base RequestConfig passed into the constructor, or
     * one built from scratch. All timeouts are set to the 
     * <code>timeoutMillis</code> parameter (unless it's null, 
     * in which case it tries to use the default timeout value: 
     * "D1Client.default.timeout" using {@link Settings#getConfiguration()}). 
     * Enabling redirects is set by the <code>followRedirect</code> 
     * parameter (unless it's null, in which case the default is true).
     * 
     * @param timeoutMillis 
     *      an Integer for the number of milliseconds to use for the
     *      connect timeout, connection request timeout, and the 
     *      socket timeout. None are set if this is null.
     * @param followRedirect
     *      a boolean that determines if redirects should be followed.
     *      The default value for this is true.
     *      
     * @return the RequestConfig based on the given <code>timeoutMillis</code>
     *      and <code>followRedirect</code> parameters. 
     */
    private RequestConfig determineRequestConfig(Integer timeoutMillis, Boolean followRedirect)
    {
        RequestConfig.Builder rcBuilder = null;
        if (this.baseRequestConfig != null)
            rcBuilder = RequestConfig.copy(this.baseRequestConfig);
        else
            rcBuilder = RequestConfig.custom();
        
        if (timeoutMillis == null)
            timeoutMillis = Settings.getConfiguration().getInteger("D1Client.default.timeout", null);
        
        if (timeoutMillis != null)
            rcBuilder.setConnectTimeout(timeoutMillis)
                .setConnectionRequestTimeout(timeoutMillis)
                .setSocketTimeout(timeoutMillis);

        if(followRedirect != null)
            rcBuilder.setRedirectsEnabled(followRedirect);
        
        return rcBuilder.build();
    }


    @Override
    public X509Session getSession() {
        return this.x509Session;
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
    
    /**
     * Sets the default timeout parameter: <b>"D1Client.default.timeout"</b>
     * to the given Integer. This parameter is applied to the {@link RequestConfig}
     * if no timeout value is provided when making the API calls.
     * 
     * @param timeout the number of milliseconds to wait before timing out. 
     *      Determines connect timeout, connection request timeout, and 
     *      socket timeout.
     */
    public void setDefaultTimeout(Integer timeout) {
        Settings.getConfiguration().setProperty(DEFAULT_TIMEOUT_PARAM, timeout);
    }
    
    /**
     * Clears the default timeout parameter: <b>"D1Client.default.timeout"</b>.
     * This parameter is usually applied to the {@link RequestConfig}
     * if no timeout value is provided when making the API calls.
     * The default timeout value determines connect timeout, 
     * connection request timeout, and socket timeout.
     */
    public void clearDefaultTimeout() {
        Settings.getConfiguration().clearProperty(DEFAULT_TIMEOUT_PARAM);
    }
}
