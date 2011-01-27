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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;

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
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.ObjectFormat;
import org.dataone.service.types.ObjectList;
import org.dataone.service.types.SystemMetadata;
import org.dataone.service.Constants;
import org.dataone.service.D1Url;

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
public class D1RestClient extends RestClient {
    
	/**
	 * Constructor to create a new instance.
	 */
	public D1RestClient() {
		super();
	}

	public ObjectList example_listObjects(AuthToken token, Date startTime,
			Date endTime, ObjectFormat objectFormat, Boolean replicaStatus,
			Integer start, Integer count) throws NotAuthorized, InvalidRequest,
			NotImplemented, ServiceFailure, InvalidToken, 
			ClientProtocolException, IOException, JiBXException, AuthenticationTimeout {


		if (endTime != null && startTime != null && !endTime.after(startTime))
			throw new InvalidRequest("1000", "startTime must be after stopTime in NMode.listObjects");

		D1Url url = new D1Url("nodeBaseServiceUrl",Constants.RESOURCE_OBJECTS);
		
		url.addNonEmptyParamPair("startTime", convertDateToGMT(startTime));
		url.addNonEmptyParamPair("endTime", convertDateToGMT(endTime));
		url.addNonEmptyParamPair("objectFormat", objectFormat.toString());
		url.addNonEmptyParamPair("replicaStatus", replicaStatus.toString());
		url.addNonEmptyParamPair("start",start.toString());
		url.addNonEmptyParamPair("count",count.toString());


		HttpResponse response = doGetRequest(url.getUrl());
		InputStream is = null;
		try {
			is = filterErrors(response);
		} catch (NotFound e) {
		} catch (IdentifierNotUnique e) {
		} catch (UnsupportedType e) {
		} catch (InsufficientResources e) {
		} catch (InvalidSystemMetadata e) {
		} catch (InvalidCredentials e) {
		} catch (IllegalStateException e) {
		} // these errors not thrown by the server

		ObjectList ol = deserializeObjectList(is);
		return ol;
	}
 
    // ==========================  New Handlers ===========================//

 
	public InputStream filterErrors(HttpResponse res) throws NotFound, InvalidToken, ServiceFailure, NotAuthorized,
	IdentifierNotUnique, UnsupportedType, InsufficientResources, InvalidSystemMetadata,
	NotImplemented, InvalidCredentials, InvalidRequest, IllegalStateException, IOException, AuthenticationTimeout {

		int code = res.getStatusLine().getStatusCode();
		if (code != HttpURLConnection.HTTP_OK) {
			// error, so throw exception
			deserializeAndThrowException(res);
		}		
		return res.getEntity().getContent();
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
	 * 
	 * @param response
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
	 * @throws AuthenticationTimeout 
	 */
    public void deserializeAndThrowException(HttpResponse response)
			throws NotFound, InvalidToken, ServiceFailure, NotAuthorized,
			NotFound, IdentifierNotUnique, UnsupportedType,
			InsufficientResources, InvalidSystemMetadata, NotImplemented,
			InvalidCredentials, InvalidRequest, IOException, AuthenticationTimeout {

    	
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
       	else if (contentType.contains("csv"))
    		ee = deserializeCsv(response);
    	else 
    		// attempt the default...
    		ee = deserializeXml(response);
    	
 
    	
    	switch (ee.getCode()) {
		// last updated 26-Jan-2011
		// Note: there are a few conflicts that don't allow us to determine the proper Exception 	
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
		case 408:
			throw new AuthenticationTimeout(ee.getDetailCode(), ee.getDescription());
    	case 409:
    		throw new IdentifierNotUnique(ee.getDetailCode(), ee.getDescription());
		case 413:
			throw new InsufficientResources(ee.getDetailCode(), ee.getDescription());
    	case 500:
    		throw new ServiceFailure(ee.getDetailCode(), ee.getDescription());
    		// TODO: Handle other exception codes properly
    	default:
    		throw new ServiceFailure(ee.getDetailCode(), ee.getDescription());
    	}

    	
    }
    	
    private ErrorElements deserializeHtml(HttpResponse response) throws IllegalStateException, IOException {
    	ErrorElements ee = new ErrorElements();
    	ee.setCode(response.getStatusLine().getStatusCode());
//    	ee.setDetailCode(detailCode);
    	String body = IOUtils.toString(response.getEntity().getContent());
    	ee.setDescription("parser for deserializing HTML not written yet.  Providing message body:\n" + body);
    	
    	return ee;
    }

    private ErrorElements deserializeJson(HttpResponse response) throws IllegalStateException, IOException {
    	ErrorElements ee = new ErrorElements();
    	
    	ee.setCode(response.getStatusLine().getStatusCode());
//    	ee.setDetailCode(detailCode);
    	String body = IOUtils.toString(response.getEntity().getContent());
    	ee.setDescription("parser for deserializing JSON not written yet.  Providing message body:\n" + body);
    	
    	return ee;
    }
  
    private ErrorElements deserializeCsv(HttpResponse response) throws IllegalStateException, IOException {
    	ErrorElements ee = new ErrorElements();
    	ee.setCode(response.getStatusLine().getStatusCode());
//    	ee.setDetailCode(detailCode);
    	String body = IOUtils.toString(response.getEntity().getContent());
    	ee.setDescription("parser for deserializing CSV not written yet.  Providing message body:\n" + body);
    	
    	return ee;
    }
   
    
    private ErrorElements deserializeXml(HttpResponse response) throws IllegalStateException, IOException
//    throws NotFound, InvalidToken, ServiceFailure, NotAuthorized,
//    NotFound, IdentifierNotUnique, UnsupportedType,
//    InsufficientResources, InvalidSystemMetadata, NotImplemented,
//    InvalidCredentials, InvalidRequest, IOException {
    {
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
