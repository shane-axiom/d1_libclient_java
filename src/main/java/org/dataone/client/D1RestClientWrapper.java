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
public abstract class D1RestClientWrapper extends RestClient {

	// TODO: This class should implement the MemberNodeAuthorization interface as well
    /** The URL string for the node REST API */
    private String nodeBaseServiceUrl;
    
	/**
	 * Constructor to create a new instance.
	 */
	public D1RestClientWrapper(String nodeBaseServiceUrl) {
	    super();
	}

	// TODO: this constructor should not exist
	// lest we end up with a client that is not attached to a particular node; 
	// No code calls it in Java, but it is called by the R client; evaluate if this can change
	/**
	 * default constructor needed by some clients.  This constructor will probably
	 * go away so don't depend on it.  Use public D1Node(String nodeBaseServiceUrl) instead.
	 */
	public D1RestClientWrapper() {
	}



	public ObjectList example_listObjects(AuthToken token, Date startTime,
			Date endTime, ObjectFormat objectFormat, Boolean replicaStatus,
			Integer start, Integer count) throws NotAuthorized, InvalidRequest,
			NotImplemented, ServiceFailure, InvalidToken {

		String resource = Constants.RESOURCE_OBJECTS;

		if (endTime != null && startTime != null && !endTime.after(startTime))
			throw new InvalidRequest("1000", "startTime must be after stopTime in NMode.listObjects");


		Hashtable paramMap = new Hashtable();
		if (startTime != null)
			paramMap.put("startTime", convertDateToGMT(startTime));
		if (endTime != null) 
			paramMap.put("endTime", convertDateToGMT(endTime));
		if (objectFormat != null)
			paramMap.put("objectFormat", objectFormat.toString());
		if (replicaStatus != null)   
			paramMap.put("replicaStatus", replicaStatus);
		if (start != null)
			paramMap.put("start",start);
		if (count != null)
			paramMap.put("count",count);



		String url = assembleAndEncodeUrl(this.getNodeBaseServiceUrl(),Constants.RESOURCE_OBJECTS, null,
				paramMap);

		HttpResponse r = handleGetRequest(url);

		ObjectList ol = null;
		try {
			ol = deserializeObjectList(r.getEntity().getContent());
		} catch (JiBXException e) {
			throw new ServiceFailure("500",
					"Could not deserialize the ObjectList: " + e.getMessage());
		} catch (IllegalStateException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ol;
	}
 
    // ==========================  New Handlers ===========================//

 
	
	
	
	private void handleResponseExceptions(HttpResponse res) throws NotFound, InvalidToken, ServiceFailure, NotAuthorized,
	IdentifierNotUnique, UnsupportedType, InsufficientResources, InvalidSystemMetadata,
	NotImplemented, InvalidCredentials, InvalidRequest, IOException {

		int code = res.getStatusLine().getStatusCode();
		if (code != HttpURLConnection.HTTP_OK) {
			// error, so throw exception
			deserializeAndThrowException(res);
		}
		if (res.getEntity() == null || res.getEntity().getContent() == null)
			throw new IOException("Unexpected empty inputStream for the request");
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
	 *  converts the serialized errorStream to the appropriate Java Exception
	 *  errorStream is not guaranteed to hold xml.  Code is passed in to preserve
	 *  http code in the case of non-dataone errors.
	 *  
	 * @param errorStream
	 * @param httpCode
	 * @throws NotFound
	 * @throws InvalidToken
	 * @throws ServiceFailure
	 * @throws NotAuthorized
	 * @throws NotFound
	 * @throws IdentifierNotUnique
	 * @throws UnsupportedType
	 * @throws InsufficientResources
	 * @throws InvalidSystemMetadata
	 * @throws NotImplemented
	 * @throws InvalidCredentials
	 * @throws InvalidRequest
	 * @throws IOException 
	 */
    protected void deserializeAndThrowException(HttpResponse response)
			throws NotFound, InvalidToken, ServiceFailure, NotAuthorized,
			NotFound, IdentifierNotUnique, UnsupportedType,
			InsufficientResources, InvalidSystemMetadata, NotImplemented,
			InvalidCredentials, InvalidRequest, IOException {

    	
    	// use content-type to determine what format the response is
    	Header[] h = response.getHeaders("content-type");
    	String contentType = null; 
    	if (h.length == 1)
    		contentType = h[0].getValue();
    	else
    		throw new IOException("Should not get more than one content-type returned");
    	
    	
    	ErrorElements ee = null;
    	if (contentType.contains("xml"))
    		ee = deserializeXml(response);
    	else if (contentType.contains("html"))
    		ee = deserializeHtml(response);
    	else if (contentType.contains("json"))
    		ee = deserializeJson(response);
    	else 
    		// attempt the default...
    		ee = deserializeXml(response);
    	
 
    	
    	switch (ee.getCode()) {
    	case 400:
    		if (ee.getDetailCode().startsWith("1180")) {
    			throw new InvalidSystemMetadata("1180", ee.getDescription());
    		} else {
    			throw new InvalidRequest(ee.getDetailCode(), ee.getDescription());
    		}
    	case 401:
    		throw new InvalidCredentials(ee.getDetailCode(), ee.getDescription());
    	case 404:
    		throw new NotFound(ee.getDetailCode(), ee.getDescription());
    	case 409:
    		throw new IdentifierNotUnique(ee.getDetailCode(), ee.getDescription());
    	case 500:
    		throw new ServiceFailure(ee.getDetailCode(), ee.getDescription());
    		// TODO: Handle other exception codes properly
    	default:
    		throw new ServiceFailure(ee.getDetailCode(), ee.getDescription());
    	}

    	
    }
    	
    private ErrorElements deserializeHtml(HttpResponse response) throws NotImplemented {
    	// TODO complete implementation of this method according to the specs.
    	throw new NotImplemented("1000","handler for deserializing HTML not written yet.");
    }

    private ErrorElements deserializeJson(HttpResponse response) throws NotImplemented {
    	// TODO complete implementation of this method according to the specs.
    	throw new NotImplemented("1000","handler for deserializing JSON not written yet.");
    }
    
    private ErrorElements deserializeXml(HttpResponse response)
    throws NotFound, InvalidToken, ServiceFailure, NotAuthorized,
    NotFound, IdentifierNotUnique, UnsupportedType,
    InsufficientResources, InvalidSystemMetadata, NotImplemented,
    InvalidCredentials, InvalidRequest, IOException {

    	ErrorElements ee = new ErrorElements();
    	
    	DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    	Document doc;

    	int httpCode = response.getStatusLine().getStatusCode();
    	BufferedInputStream bErrorStream = new BufferedInputStream(response.getEntity().getContent());
    	bErrorStream.mark(5000);  // good for resetting up to 5000 bytes	

    	String detailCode = null;
    	String description = null;
    	int errorCode = -1;
    	try {
    		DocumentBuilder db = dbf.newDocumentBuilder();
    		doc = db.parse(bErrorStream);
    		Element root = doc.getDocumentElement();
    		root.normalize();
    		try {
    			errorCode = getIntAttribute(root, "errorCode");
    		} catch (NumberFormatException nfe){
    			System.out.println("errorCode unexpectedly not able to parse to int," +
    					" using http status for creating exception");
    			errorCode = httpCode;
    		}    		
    		if (errorCode != httpCode)
//				throw new ServiceFailure("1000","errorCode in message body doesn't match httpStatus");
				System.out.println("errorCode in message body doesn't match httpStatus," +
						" using errorCode for creating exception");
    		
    		
    		detailCode = root.getAttribute("detailCode");
    		description = getTextValue(root, "description");

    	} catch (SAXException e) {
    		description = deserializeNonXMLErrorStream(bErrorStream,e);
    	} catch (IOException e) {
    		description = deserializeNonXMLErrorStream(bErrorStream,e);
    	} catch (ParserConfigurationException e) {
    		description = deserializeNonXMLErrorStream(bErrorStream,e);
    	}

    	ee.setCode(errorCode);
    	ee.setDetailCode(detailCode);
    	ee.setDescription(description);
    	
    	return ee;
    }

    /*
     * helper method for deserializeAndThrowException.  Used for problems parsing errorStream as XML
     */
    private String deserializeNonXMLErrorStream(BufferedInputStream errorStream, Exception e) 
    {
    	String errorString = null;
    	try {
    		errorStream.reset();
    		errorString = e.getMessage() + "\n" + IOUtils.toString(errorStream);
    	} catch (IOException e1) {
    		errorString = "errorStream could not be reset/reread";
    	}
    	return errorString;
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

	
	//  === dataone datatype handlers
	
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
}
