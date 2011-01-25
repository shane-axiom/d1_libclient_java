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
public abstract class RestClient {

	// TODO: This class should implement the MemberNodeAuthorization interface as well
    /** The URL string for the node REST API */
    private String nodeBaseServiceUrl;
    
	/**
	 * Constructor to create a new instance.
	 */
	public RestClient(String nodeBaseServiceUrl) {
	    setNodeBaseServiceUrl(nodeBaseServiceUrl);
	}

	// TODO: this constructor should not exist
	// lest we end up with a client that is not attached to a particular node; 
	// No code calls it in Java, but it is called by the R client; evaluate if this can change
	/**
	 * default constructor needed by some clients.  This constructor will probably
	 * go away so don't depend on it.  Use public D1Node(String nodeBaseServiceUrl) instead.
	 */
	public RestClient() {
	}


    /**
     * Retrieve the service URL for this node.  The service URL can be used with
     * knowledge of the DataONE REST API to construct endpoints for each of the
     * DataONE REST services that are available on the node.
     * @return String representing the service URL
     */
    public String getNodeBaseServiceUrl() {
        return this.nodeBaseServiceUrl;
    }

    /**
     * Set the service URL for this node.  The service URL can be used with
     * knowledge of the DataONE REST API to construct endpoints for each of the
     * DataONE REST services that are available on the node.
     * @param nodeBaseServiceUrl String representing the service URL
     */
    public void setNodeBaseServiceUrl(String nodeBaseServiceUrl) {
        if (!nodeBaseServiceUrl.endsWith("/")) {
            nodeBaseServiceUrl = nodeBaseServiceUrl + "/";
        }
        this.nodeBaseServiceUrl = nodeBaseServiceUrl;
    }

    
    // ==========================  New Handlers ===========================//

    /**
     * assembles url components into a properly encoded complete url
     */
	protected static String assembleAndEncodeUrl(String baseURL, String resource, 
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
     * @throws ServiceFailure 
	 */
	public HttpResponse handleGetRequest(String url) throws ServiceFailure {
		return doRequestNoBody(url,Constants.GET);
	}

    /**
	 * send a Head request to the resource and get the response
     * @throws ServiceFailure 
	 */

	public HttpResponse handleHeadRequest(String url) throws ServiceFailure {
		return doRequestNoBody(url,Constants.HEAD);
	}

    /**
	 * send a Delete request to the resource and get the response
     * @throws ServiceFailure 
	 */

	public HttpResponse handleDeleteRequest(String url) throws ServiceFailure {
		return doRequestNoBody(url,Constants.DELETE);
	}
	
	
    /**
	 * send a POST request to the resource and get the response
     * @throws ServiceFailure 
	 */
	// TODO: figure out how to get the multipart bits into the method signature
	public HttpResponse doPostRequest(String url) throws ServiceFailure {
		MultipartRequestHandler h = new MultipartRequestHandler(url, Constants.POST);
		HttpResponse resp = null;
		try {
			resp = h.executeRequest();
		} catch (ClientProtocolException e) {
			throw new ServiceFailure("1000",e.getClass() + " ERROR:" + e.getMessage());
		} catch (IOException e) {
			throw new ServiceFailure("1000",e.getClass() + " ERROR:" + e.getMessage());
		}
		return resp;
	}

    /**
	 * send a PUT request to the resource and get the response
     * @throws ServiceFailure 
	 */
	// TODO: figure out how to get the multipart bits into the method signature
	public HttpResponse doPutRequest(String url) throws ServiceFailure {
		MultipartRequestHandler h = new MultipartRequestHandler(url, Constants.PUT);
		HttpResponse resp = null;
		try {
			resp = h.executeRequest();
		} catch (ClientProtocolException e) {
			throw new ServiceFailure("1000",e.getClass() + " ERROR:" + e.getMessage());
		} catch (IOException e) {
			throw new ServiceFailure("1000",e.getClass() + " ERROR:" + e.getMessage());
		}
		return resp;
	}
	
	private HttpResponse doRequestNoBody(String url,String httpMethod) throws ServiceFailure {

		HttpUriRequest method = null;

		DefaultHttpClient httpClient = new DefaultHttpClient();
		if (httpMethod == Constants.GET) 
			method = new HttpGet(url);        	
		if (httpMethod == Constants.HEAD) 
			method = new HttpHead(url);       
		if (httpMethod == Constants.DELETE)
			method = new HttpDelete(url);       

		HttpResponse resp = null;
		try {
			resp = httpClient.execute(method);
		} catch (ClientProtocolException e) {
			throw new ServiceFailure("1000",e.getClass() + " ERROR:" + e.getMessage());
		} catch (IOException e) {
			throw new ServiceFailure("1000",e.getClass() + " ERROR:" + e.getMessage());
		}
		return resp;
	}
	
	
	


	
	protected Identifier parseResponseForIdentifer(HttpResponse resp) throws ServiceFailure {
		
		Identifier id = null;
		try {
			InputStream is = resp.getEntity().getContent();
			Header type = resp.getEntity().getContentType();
			System.out.println("Response content-type: "+ type.getValue());
			if (is != null)
				id = (Identifier)deserializeServiceType(Identifier.class, is);
			else
				throw new IOException("Unexpected empty inputStream for the request");
		}
		catch (JiBXException e) {
			throw new ServiceFailure("500",
					"Could not deserialize the returned Identifier: " + e.getMessage());
		}
		catch (IOException e) {
			throw new ServiceFailure("1000",
					"IOException in processing: " + e.getMessage());
		}
		return id;
	}
	
	

	
// ========================  original handlers ==============================//
    
  
	/**
	 * convert a date to GMT
	 * 
	 * @param d
	 * @return
	 */
	protected String convertDateToGMT(Date d) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT-0"));
		String s = dateFormat.format(d);
		return s;
	}
	


	

	
	



    
    
	/**
	 * Take a xml element and the tag name, return the text content of the child
	 * element.
	 */
	protected String getTextValue(Element e, String tag) {
		String text = null;
		NodeList nl = e.getElementsByTagName(tag);
		if (nl != null && nl.getLength() > 0) {
			Element el = (Element) nl.item(0);
			text = el.getFirstChild().getNodeValue();
		}

		return text;
	}

	/**
	 * return an xml attribute as an int
	 * @param e
	 * @param attName
	 * @return
	 */
	protected int getIntAttribute(Element e, String attName)
	throws NumberFormatException {
		String attText = e.getAttribute(attName);
		int x = Integer.parseInt(attText);
		return x;
	}

	/**
	 * Serialize the system metadata object to a ByteArrayInputStream
	 * @param sysmeta
	 * @return
	 * @throws JiBXException
	 */
	protected ByteArrayInputStream serializeSystemMetadata(SystemMetadata sysmeta)
			throws JiBXException {

		/*
		 * IBindingFactory bfact =
		 * BindingDirectory.getFactory(SystemMetadata.class);
		 * IMarshallingContext mctx = bfact.createMarshallingContext();
		 * ByteArrayOutputStream sysmetaOut = new ByteArrayOutputStream();
		 * mctx.marshalDocument(sysmeta, "UTF-8", null, sysmetaOut);
		 * ByteArrayInputStream sysmetaStream = new
		 * ByteArrayInputStream(sysmetaOut.toByteArray());
                 * return sysmetaStream;
		 */

		ByteArrayOutputStream sysmetaOut = new ByteArrayOutputStream();
		serializeServiceType(SystemMetadata.class, sysmeta, sysmetaOut);
		ByteArrayInputStream sysmetaStream = new ByteArrayInputStream(
				sysmetaOut.toByteArray());
		return sysmetaStream;
	}

	/**
	 * deserialize an InputStream to a SystemMetadata object
	 * @param is
	 * @return
	 * @throws JiBXException
	 */
	protected SystemMetadata deserializeSystemMetadata(InputStream is)
			throws JiBXException {
		return (SystemMetadata) deserializeServiceType(SystemMetadata.class, is);
	}

	/**
	 * deserialize and ObjectList from an InputStream
	 * @param is
	 * @return
	 * @throws JiBXException
	 */
	protected ObjectList deserializeObjectList(InputStream is)
			throws JiBXException {
		return (ObjectList) deserializeServiceType(ObjectList.class, is);
	}

	/**
	 * serialize an object of type to out
	 * 
	 * @param type
	 *            the class of the object to serialize (i.e.
	 *            SystemMetadata.class)
	 * @param object
	 *            the object to serialize
	 * @param out
	 *            the stream to serialize it to
	 * @throws JiBXException
	 */
	@SuppressWarnings("rawtypes")
	protected void serializeServiceType(Class type, Object object,
			OutputStream out) throws JiBXException {
		IBindingFactory bfact = BindingDirectory.getFactory(type);
		IMarshallingContext mctx = bfact.createMarshallingContext();
		mctx.marshalDocument(object, "UTF-8", null, out);
	}

	/**
	 * deserialize an object of type from is
	 * 
	 * @param type
	 *            the class of the object to serialize (i.e.
	 *            SystemMetadata.class)
	 * @param is
	 *            the stream to deserialize from
	 * @throws JiBXException
	 */
	@SuppressWarnings("rawtypes")
	protected Object deserializeServiceType(Class type, InputStream is)
			throws JiBXException {
		IBindingFactory bfact = BindingDirectory.getFactory(type);
		IUnmarshallingContext uctx = bfact.createUnmarshallingContext();
		Object o = (Object) uctx.unmarshalDocument(is, null);
		return o;
	}

	public class ErrorElements {
		private int code;
		private String detailCode;
		private String description;

		protected ErrorElements() {
			super();
		}
	
		protected int getCode() {
			return code;
		}
		
		protected void setCode(int c) {
			this.code = c;
		}

		protected String getDetailCode() {
			return detailCode;
		}
		
		protected void setDetailCode(String dc) {
			this.detailCode = dc;
		}

		protected String getDescription() {
			return this.description;
		}
		
		protected void setDescription(String d) {
			this.description = d;
		}
	}
	

//	/**
//	 * A class to contain the data from a server response
//	 */
//	public class ResponseData {
//		private int code;
//		private InputStream contentStream;
//		private InputStream errorStream;
//		private Map<String, List<String>> headerFields;
//
//		/**
//		 * constructor
//		 */
//		protected ResponseData() {
//			super();
//		}
//
//		/**
//		 * @return the code
//		 */
//		protected int getCode() {
//			return code;
//		}
//		
//		/**
//		 * set the header fields from the response
//		 * @param m
//		 */
//		protected void setHeaderFields(Map<String, List<String>> m)
//		{
//		    this.headerFields = m;
//		}
//		
//		/**
//		 * get the header fields associated with this response
//		 * @return Map<String, List<String>>
//		 */
//		protected Map<String, List<String>> getHeaderFields()
//		{
//		    return this.headerFields;
//		}
//
//		/**
//		 * @param code
//		 *            the code to set
//		 */
//		protected void setCode(int code) {
//			this.code = code;
//		}
//
//		/**
//		 * @return the contentStream
//		 */
//		protected InputStream getContentStream() {
//			return contentStream;
//		}
//
//		/**
//		 * @param contentStream
//		 *            the contentStream to set
//		 */
//		protected void setContentStream(InputStream contentStream) {
//			this.contentStream = contentStream;
//		}
//
//		/**
//		 * @return the errorStream
//		 */
//		protected InputStream getErrorStream() {
//			return errorStream;
//		}
//
//		/**
//		 * @param errorStream
//		 *            the errorStream to set
//		 */
//		protected void setErrorStream(InputStream errorStream) {
//			this.errorStream = errorStream;
//		}
//	}

}
