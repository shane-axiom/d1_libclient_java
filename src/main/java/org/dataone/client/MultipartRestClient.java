package org.dataone.client;

import java.io.InputStream;

import org.apache.http.Header;
import org.dataone.client.exception.ClientSideException;
import org.dataone.mimemultipart.SimpleMultipartEntity;
import org.dataone.service.exceptions.BaseException;

/**
 * The primary interface to use when constructing DataONE API requests.
 * It encapsulates the requirements for RESTful services and use of 
 * Mime Multipart packaging of POST and PUT requests, as well as use of
 * DataONE exception types.
 * 
 * IMPORTANT: Users are responsible for closing the input streams returned 
 * by method implementations.
 * 
 * @author rnahf
 *
 */
public interface MultipartRestClient {

	/**
	 * Perform an HTTP GET request, setting the headers first and parsing /filtering
	 * exceptions to the exception stream on the response into their
	 * respective java instances.
	 * 
	 * @param url - the encoded url string
	 * @return the InputStream from the http Response
	 * 
	 * @throws BaseException
	 * @throws ClientSideException
	 */
	public InputStream doGetRequest(String url) 
		throws BaseException, ClientSideException;

	public InputStream doGetRequest(String url, boolean allowRedirect)
		throws BaseException, ClientSideException;

	// TODO: remove Header from the signature to remove dependency on org.apache.http
	public Header[] doGetRequestForHeaders(String url)
		throws BaseException, ClientSideException;

	public InputStream doDeleteRequest(String url)
		throws BaseException, ClientSideException;

	public Header[] doHeadRequest(String url) 
		throws BaseException, ClientSideException;

	public InputStream doPutRequest(String url, SimpleMultipartEntity entity) 
		throws BaseException, ClientSideException;

	public InputStream doPostRequest(String url, SimpleMultipartEntity entity) 
		throws BaseException, ClientSideException;

}