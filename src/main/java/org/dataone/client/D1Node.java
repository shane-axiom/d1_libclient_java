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
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.httpclient.params.DefaultHttpParams;
import org.apache.commons.httpclient.params.HttpConnectionParams;
import org.apache.commons.io.IOUtils;
import org.dataone.mimemultipart.MultipartRequestHandler;
import org.dataone.service.Constants;
import org.dataone.service.EncodingUtilities;
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
import org.dataone.service.types.util.ServiceTypeUtil;
import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IMarshallingContext;
import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.params.HttpParams;

/**
 * An abstract node class that contains base functionality shared between 
 * Coordinating Node and Member Node implementations. 
 */
public abstract class D1Node {

	// TODO: This class should implement the MemberNodeAuthorization interface as well
    /** The URL string for the node REST API */
    private String nodeBaseServiceUrl;
    
	/**
	 * Constructor to create a new instance.
	 */
	public D1Node(String nodeBaseServiceUrl) {
	    setNodeBaseServiceUrl(nodeBaseServiceUrl);
	}

	// TODO: this constructor should not exist
	// lest we end up with a client that is not attached to a particular node; 
	// No code calls it in Java, but it is called by the R client; evaluate if this can change
	/**
	 * default constructor needed by some clients.  This constructor will probably
	 * go away so don't depend on it.  Use public D1Node(String nodeBaseServiceUrl) instead.
	 */
	public D1Node() {
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
    
    /**
     *   listObjects is the simple implementation of /<service>/object (no query parameters, or additional path segments)
     *   use this when no parameters  being used
     *   
     * @param token
     * @return
     * @throws NotAuthorized
     * @throws InvalidRequest
     * @throws NotImplemented
     * @throws ServiceFailure
     * @throws InvalidToken
     */
    public ObjectList listObjects()
    throws NotAuthorized, InvalidRequest, NotImplemented, ServiceFailure, InvalidToken {
  
    	return listObjects(null,null,null,null,null,null,null);
        
//        InputStream is;
//		try {
//			is = handleHttpGet(null,resource);
//		} catch (NotFound eNF) {
//			// convert notfound error to service failure
//			// because it is not valid in the listObjects case
//			throw new ServiceFailure("1000", "Method threw unexpected exception: " + eNF.getMessage());
//		}
//         
//        try {
//            return deserializeObjectList(is);
//        } catch (JiBXException e) {
//            throw new ServiceFailure("500",
//                    "Could not deserialize the ObjectList: " + e.getMessage());
//        }
    }
    
//    public ObjectList listObjects()
//    throws NotAuthorized, InvalidRequest, NotImplemented, ServiceFailure, InvalidToken {
//    	return listObjects(null);
//    }
    
    /**
     * Get the resource with the specified guid.  Used by both the CNode and 
     * MNode implementations.
     */
    public InputStream get(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        String resource = Constants.RESOURCE_OBJECTS + "/" + EncodingUtilities.encodeUrlPathSegment(guid.getValue());
 
        return handleHttpGet(token,resource);
    }
    
    /**
     * Get the system metadata from a resource with the specified guid. Used
     * by both the CNode and MNode implementations.
     */
    public SystemMetadata getSystemMetadata(AuthToken token, Identifier guid)
     throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
     InvalidRequest, NotImplemented {

        String resource = Constants.RESOURCE_META + "/" + EncodingUtilities.encodeUrlPathSegment(guid.getValue());
        InputStream is = handleHttpGet(token,resource);
        
        try {
            return deserializeSystemMetadata(is);
        } catch (Exception e) {
            throw new ServiceFailure("1090",
                    "Could not deserialize the systemMetadata: " + e.getMessage());
        }
    }

    
    public ObjectList listObjects(AuthToken token, Date startTime,
            Date endTime, ObjectFormat objectFormat, Boolean replicaStatus,
            Integer start, Integer count) throws NotAuthorized, InvalidRequest,
            NotImplemented, ServiceFailure, InvalidToken {
        
    	String resource = Constants.RESOURCE_OBJECTS;

        if (endTime != null && startTime != null && !endTime.after(startTime))
            throw new InvalidRequest("1000", "startTime must be after stopTime in NMode.listObjects");

        // TODO: Check that the format is valid, throw InvalidRequest if not
        String params = "";
        if (startTime != null)
            params += "startTime=" + convertDateToGMT(startTime) + "&";
        if (endTime != null) 
        	params += "endTime=" + convertDateToGMT(endTime) + "&";
        if (objectFormat != null)
        	params += "objectFormat=" + objectFormat + "&";
        if (replicaStatus != null)   
        	params += "replicaStatus=" + replicaStatus + "&";
        if (start != null)
        	params += "start=" + start + "&";
        if (count != null)
        	params += "count=" + count + "&";

        // clean up param string
        if (params.endsWith("&"))
        	params = params.substring(0, params.length()-1);
                   
        InputStream is;
		try {
			is = handleHttpGet(token,resource,params);
		} catch (NotFound eNF) {
			// convert notfound error to service failure
			// because it is not valid in the listObjects case
			throw new ServiceFailure("1000", "Method threw unexpected exception: " + eNF.getMessage());
		}
         
        try {
            return deserializeObjectList(is);
        } catch (JiBXException e) {
            throw new ServiceFailure("500",
                    "Could not deserialize the ObjectList: " + e.getMessage());
        }
    }
    
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
	 * send a request to the resource and get the response
	 */
	
	protected ResponseData sendRequest(AuthToken token, String resource,
			String method, String urlParameters, String contentType,
			InputStream dataStream, String acceptHeader) throws ServiceFailure {

		ResponseData resData = new ResponseData();
		HttpURLConnection connection = null;

		String restURL = nodeBaseServiceUrl + resource;
		
		// normalize empty string and all-whitespace strings to the null case
		if (urlParameters != null) 
		{
			if (urlParameters.trim().length() == 0)
				urlParameters = null;
			else {
				if (restURL.indexOf("?") == -1)
					restURL += "?";
				restURL += urlParameters;
			}
		}
		// convert all internal whitespace to escaped space
		restURL = restURL.replaceAll("\\s", "%20");

		if (token != null) {
			if (restURL.indexOf("?") == -1) {
				restURL += "?sessionid=" + token.getToken();
			} else {
				restURL += "&sessionid=" + token.getToken();
			}
		}

		URL u = null;
		InputStream content = null;
		try {

			if (restURL.indexOf("+") != -1) {
				restURL = restURL.replaceAll("\\+", "%2b");
			}
			System.out.println("restURL: " + restURL);
			System.out.println("method: " + method);

			u = new URL(restURL);
			connection = (HttpURLConnection) u.openConnection();
			if (contentType != null) {
				connection.setRequestProperty("Content-Type", contentType);
			}
			if (acceptHeader != null) {
			    connection.setRequestProperty("Accept", acceptHeader);    
			}

			connection.setDoOutput(true);
			connection.setDoInput(true);
			connection.setRequestMethod(method);
			connection.connect();
			
			if (!method.equals(Constants.GET)) {
				if (dataStream != null) {
					OutputStream out = connection.getOutputStream();
					IOUtils.copy(dataStream, out);
				}
			}

			try {
				content = connection.getInputStream();
				resData.setContentStream(content);
			} catch (IOException ioe) {
				// the connection error stream contains the error xml from metacat
				// make note that an error stream is being returned instead.
				System.out.println("tried to get content and failed.  Receiving an error stream instead.");
				//+
					//	" connection error stream: " + IOUtils.toString(connection.getErrorStream()));
//				resData.setCode(HttpURLConnection.HTTP_INTERNAL_ERROR);				
//				resData.setErrorStream(connection.getErrorStream());

				// going to map the exception's error message to the response's error stream
//				ByteArrayInputStream inputStream = new ByteArrayInputStream(ioe.getMessage().getBytes());					
//				resData.setErrorStream(inputStream);
			}

			int code = connection.getResponseCode();
			resData.setCode(code);
			resData.setHeaderFields(connection.getHeaderFields());
			if (code != HttpURLConnection.HTTP_OK) 
				resData.setErrorStream(connection.getErrorStream());
			
		} catch (MalformedURLException e) {
			throw new ServiceFailure("1000", restURL + " " + e.getMessage());
		} catch (ProtocolException e) {
			throw new ServiceFailure("1000", restURL + " " + e.getMessage());
		} catch (FileNotFoundException e) {
			resData.setCode(HttpURLConnection.HTTP_NOT_FOUND);
			resData.setErrorStream(connection.getErrorStream());
		} catch (IOException e) {
			e.printStackTrace();
			throw new ServiceFailure("1000", restURL + " " + e.getMessage());
		}
		return resData;
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
	 */
    protected void deserializeAndThrowException(int httpCode, InputStream errorStream)
			throws NotFound, InvalidToken, ServiceFailure, NotAuthorized,
			NotFound, IdentifierNotUnique, UnsupportedType,
			InsufficientResources, InvalidSystemMetadata, NotImplemented,
			InvalidCredentials, InvalidRequest {

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		Document doc;

		BufferedInputStream bErrorStream = new BufferedInputStream(errorStream);
		bErrorStream.mark(100000);  // good for resetting up to 100000 bytes	
		
		String detailCode = null;
		String description = null;
		try {
			DocumentBuilder db = dbf.newDocumentBuilder();
			doc = db.parse(bErrorStream);
			Element root = doc.getDocumentElement();
			root.normalize();
			try {
				httpCode = getIntAttribute(root, "errorCode");
			} catch (NumberFormatException nfe){
				System.out.println("  errorCode unexpectedly not able to parse to int");
			}
			detailCode = root.getAttribute("detailCode");
			description = getTextValue(root, "description");
			
		} catch (SAXException e) {
			description = deserializeNonXMLErrorStream(bErrorStream,e);
		} catch (IOException e) {
			description = deserializeNonXMLErrorStream(bErrorStream,e);
		} catch (ParserConfigurationException e) {
			description = deserializeNonXMLErrorStream(bErrorStream,e);
		}
		
		switch (httpCode) {
		case 400:
		    // TODO: change this to a startsWith since error codes can have text after the number.
			if (detailCode.equals("1180")) {
				throw new InvalidSystemMetadata("1180", description);
			} else {
				throw new InvalidRequest(detailCode, description);
			}
		case 401:
			throw new InvalidCredentials(detailCode, description);
		case 404:
			throw new NotFound(detailCode, description);
		case 409:
			throw new IdentifierNotUnique(detailCode, description);
		case 500:
			throw new ServiceFailure(detailCode, description);
			// TODO: Handle other exception codes properly
		default:
			throw new ServiceFailure(detailCode, description);
		}
	}

    /*
     * helper method for deserializeAndThrowException.  Used for problems parsing errorStream as XML
     */
    private String deserializeNonXMLErrorStream(BufferedInputStream errorStream, Exception e) 
    {
    	String errorString = null;
    	try {
    		errorStream.reset();
    		errorString = IOUtils.toString(errorStream);
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
		ServiceTypeUtil.serializeServiceType(type, object, out);
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
		return ServiceTypeUtil.deserializeServiceType(type, is);
	}

	protected InputStream handleHttpGet(AuthToken token, String resource)
	 throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented	
	 {
		return handleHttpGet(token,resource, null);
	 }

	
	/**
	 *  internal service method to standardize exception handling for http GET calls
	 *  Needs to handle html error returns as well as xml
	 *  
	 * @param token
	 * @param resource
	 * @return
	 * @throws InvalidToken
	 * @throws ServiceFailure
	 * @throws NotAuthorized
	 * @throws NotFound
	 * @throws NotImplemented
	 */
	protected InputStream handleHttpGet(AuthToken token, String resource, String param)
	 throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
	{
     InputStream is = null;
     ResponseData rd = sendRequest(token, resource, Constants.GET, param, null, null, null);
     
     // First handle any errors that were generated	
     int code = rd.getCode();
     if (code != HttpURLConnection.HTTP_OK) {
         InputStream errorStream = rd.getErrorStream();
         try {
             deserializeAndThrowException(code,errorStream);
         } catch (InvalidToken e) {
             throw e;
         } catch (ServiceFailure e) {
             throw e;
         } catch (NotAuthorized e) {
             throw e;
         } catch (NotFound e) {
             throw e;
         } catch (NotImplemented e) {
             throw e;
         } catch (BaseException e) {
             throw new ServiceFailure("1000",
                     "Method threw improper exception: " + e.getMessage());
         }
     } else {
    	// Non-http-error cases
         is = rd.getContentStream();
     }
     return is;
	}
	
	
	/**
     * set action="create" for insert and action="update" for update
     */
    protected Identifier handleCreateOrUpdate(AuthToken token, Identifier guid,
        final InputStream object, final SystemMetadata sysmeta, Identifier obsoletedGuid, String action)
        throws InvalidToken,ServiceFailure, NotAuthorized, IdentifierNotUnique,
            UnsupportedType, InsufficientResources, InvalidSystemMetadata,
            NotImplemented
    {
        String resource = Constants.RESOURCE_OBJECTS + "/" + 
            EncodingUtilities.encodeUrlPathSegment(guid.getValue()) + 
            "?sessionid=" + token.getToken();

        File outputFile = null;
        InputStream multipartStream;
        HttpResponse response = null;
        HttpUriRequest request = null;
        try
        {
            Date d = new Date();
            File tmpDir = new File(Constants.TEMP_DIR);
            MultipartRequestHandler mrhandler = null;
            String serviceUrl = nodeBaseServiceUrl + resource;
            System.out.println("requesting create or update to url " + serviceUrl);
            if(action.equals("create"))
            {
                mrhandler = new MultipartRequestHandler(serviceUrl, Constants.POST);
            }
            else if(action.equals("update"))
            {
                if(obsoletedGuid == null)
                {
                    throw new NullPointerException("obsoletedGuid must not be null in MNode.update");
                }
                mrhandler = new MultipartRequestHandler(nodeBaseServiceUrl, Constants.PUT);
                mrhandler.addParamPart("obsoletedGuid", 
                        EncodingUtilities.encodeUrlQuerySegment(obsoletedGuid.getValue()));
            }
            
            //write object and sysmeta to mmp files
            mrhandler.addFilePart(object, "object");
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            serializeServiceType(SystemMetadata.class, sysmeta, baos);
            mrhandler.addFilePart("sysmeta", baos.toString());
            //why the following code to add the sessionid does not work, i do not know
            //DefaultHttpParams params = new DefaultHttpParams();
            //HttpParams httpparams = params.setParameter("sessionid", token.getToken());
            //mrhandler.getRequest().setParams(httpparams);
            
            response = mrhandler.executeRequest();
            request = mrhandler.getRequest();
        }
        catch(Exception e)
        {
            outputFile.delete();
            throw new ServiceFailure("1000", 
                    "Error creating MMP stream in MNode.handleCreateOrUpdate: " + 
                    e.getMessage() + " " + e.getStackTrace());
        }
        
 
        // First handle any errors that were generated
        String statusLine = response.getStatusLine().toString();
        System.out.println("response status: " + statusLine);
        
        /*if (statusLine.indexOf("200") == -1) {
            try {
                //deserializeAndThrowException(code,errorStream);
            } catch (InvalidToken e) {
                throw e;
            } catch (ServiceFailure e) {
                throw e;
            } catch (NotAuthorized e) {
                throw e;
            } catch (IdentifierNotUnique e) {
                throw e;
            } catch (UnsupportedType e) {
                throw e;
            } catch (InsufficientResources e) {
                throw e;
            } catch (InvalidSystemMetadata e) {
                throw e;
            } catch (NotImplemented e) {
                throw e;
            } catch (BaseException e) {
                throw new ServiceFailure("1000",
                        "Method threw improper exception: " + e.getMessage());
            } finally {
                outputFile.delete();
            }   
        } */
        
        // Now only handling non-http-error cases
        /*InputStream is = rd.getContentStream();
        Identifier id = null;
       	try
       	{
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
       	finally {
       		outputFile.delete();
       	}
       	
        outputFile.delete();
        return id;*/
        return guid;
    }

	/**
	 * A class to contain the data from a server response
	 */
	public class ResponseData {
		private int code;
		private InputStream contentStream;
		private InputStream errorStream;
		private Map<String, List<String>> headerFields;

		/**
		 * constructor
		 */
		protected ResponseData() {
			super();
		}

		/**
		 * @return the code
		 */
		protected int getCode() {
			return code;
		}
		
		/**
		 * set the header fields from the response
		 * @param m
		 */
		protected void setHeaderFields(Map<String, List<String>> m)
		{
		    this.headerFields = m;
		}
		
		/**
		 * get the header fields associated with this response
		 * @return Map<String, List<String>>
		 */
		protected Map<String, List<String>> getHeaderFields()
		{
		    return this.headerFields;
		}

		/**
		 * @param code
		 *            the code to set
		 */
		protected void setCode(int code) {
			this.code = code;
		}

		/**
		 * @return the contentStream
		 */
		protected InputStream getContentStream() {
			return contentStream;
		}

		/**
		 * @param contentStream
		 *            the contentStream to set
		 */
		protected void setContentStream(InputStream contentStream) {
			this.contentStream = contentStream;
		}

		/**
		 * @return the errorStream
		 */
		protected InputStream getErrorStream() {
			return errorStream;
		}

		/**
		 * @param errorStream
		 *            the errorStream to set
		 */
		protected void setErrorStream(InputStream errorStream) {
			this.errorStream = errorStream;
		}
	}

}
