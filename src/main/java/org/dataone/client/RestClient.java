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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TimeZone;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.dataone.mimemultipart.MultipartRequestHandler;
import org.dataone.service.exceptions.BaseException;
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
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.ObjectFormat;
import org.dataone.service.types.ObjectList;
import org.dataone.service.types.SystemMetadata;
import org.dataone.service.Constants;
import org.dataone.service.EncodingUtilities;
import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IMarshallingContext;
import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * A generic client class that contains base functionality for making REST calls
 * to remote REST servers.  
 * It is built to encapsulate the communication conventions dataONE is following
 * but does not implement the dataONE REST api itself.
 */
public class RestClient {

    /** The URL string for the node REST API */
    private DefaultHttpClient httpClient;
    
	/**
	 * Constructor to create a new instance.
	 */
	public RestClient() {
	    httpClient = new DefaultHttpClient();
	}

    // ==========================  New Handlers ===========================//

    /**
     * assembles url components into a properly encoded complete url
     */
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
		Vector<String> params = null;
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
		}
		if(!params.isEmpty()) 
			url += "?" + params.toString();
		
		return url;
	}
    
    /**
	 * send a GET request to the resource and get the response
     * @throws IOException 
     * @throws ClientProtocolException 
     * @throws ServiceFailure 
	 */
	public HttpResponse doGetRequest(String url) throws ClientProtocolException, IOException  {
		return doRequestNoBody(url,Constants.GET);
	}

    /**
	 * send a Head request to the resource and get the response
     * @throws IOException 
     * @throws ClientProtocolException 
     * @throws ServiceFailure 
	 */

	public HttpResponse doHeadRequest(String url) throws ClientProtocolException, IOException  {
		return doRequestNoBody(url,Constants.HEAD);
	}

    /**
	 * send a Delete request to the resource and get the response
     * @throws ServiceFailure 
     * @throws IOException 
     * @throws ClientProtocolException 
	 */

	public HttpResponse doDeleteRequest(String url) throws ClientProtocolException, IOException {
		return doRequestNoBody(url,Constants.DELETE);
	}
	
	
    /**
	 * send a POST request to the resource and get the response
     * @throws IOException 
     * @throws ClientProtocolException  
	 */
	// TODO: figure out how to get the multipart bits into the method signature
	public HttpResponse doPostRequest(String url, MultipartEntity mpe) throws ClientProtocolException, IOException   {
		return doRequestMMBody(url,Constants.POST,mpe);
	}

    /**
	 * send a PUT request to the resource and get the response
     * @throws IOException 
     * @throws ClientProtocolException 
	 */
	// TODO: figure out how to get the multipart bits into the method signature
	public HttpResponse doPutRequest(String url, MultipartEntity mpe) throws ClientProtocolException, IOException  {
		return doRequestMMBody(url,Constants.PUT,mpe);
	}
	
	
	private HttpResponse doRequestNoBody(String url,String httpMethod) throws ClientProtocolException, IOException  {

		HttpUriRequest req = null;
		if (httpMethod == Constants.GET) 
			req = new HttpGet(url);        	
		if (httpMethod == Constants.HEAD) 
			req = new HttpHead(url);       
		if (httpMethod == Constants.DELETE)
			req = new HttpDelete(url);       
		return httpClient.execute(req);
	}
	
	private HttpResponse doRequestMMBody(String url,String httpMethod, MultipartEntity mpe) throws ClientProtocolException, IOException {
		HttpEntityEnclosingRequestBase req = null;
		if (httpMethod == Constants.PUT)
			req = new HttpPut(url);
		if (httpMethod == Constants.POST)
			req = new HttpPost(url);
	
		if (mpe != null)
			req.setEntity(mpe);
		
		return httpClient.execute(req);
		
	}
}
