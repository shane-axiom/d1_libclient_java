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

import org.apache.commons.io.IOUtils;
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
     * Get the resource with the specified guid.  Used by both the CNode and 
     * MNode implementations.
     */
    public InputStream get(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        String resource = Constants.RESOURCE_OBJECTS + "/" + EncodingUtilities.encodeUrlPathSegment(guid.getValue());
        InputStream is = null;
        ResponseData rd = sendRequest(token, resource, Constants.GET, null, null, null, null);
        int code = rd.getCode();
        if (code != HttpURLConnection.HTTP_OK) {
            InputStream errorStream = rd.getErrorStream();
            try {
                deserializeAndThrowException(errorStream);
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
            is = rd.getContentStream();
        }

        return is;
    }
    
    /**
     * Get the system metadata from a resource with the specified guid. Used
     * by both the CNode and MNode implementations.
     */
    public SystemMetadata getSystemMetadata(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            InvalidRequest, NotImplemented {

        String resource = Constants.RESOURCE_META + "/" + EncodingUtilities.encodeUrlPathSegment(guid.getValue());
        InputStream is = null;
        ResponseData rd = sendRequest(token, resource, Constants.GET, null, null, null, null);
        int code = rd.getCode();
        if (code != HttpURLConnection.HTTP_OK) {
            InputStream errorStream = rd.getErrorStream();
            try {
                deserializeAndThrowException(errorStream);
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
            is = rd.getContentStream();
        }

        // TODO: this block should be inside the above else conditional, right?
        try {
            return deserializeSystemMetadata(is);
        } catch (Exception e) {
            throw new ServiceFailure("1090",
                    "Could not deserialize the systemMetadata: "
                            + e.getMessage());
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
	 * create a mime multipart message from object and sysmeta and write it to out
	 */
	protected void createMimeMultipart(OutputStream out, InputStream object,
			SystemMetadata sysmeta) throws Exception
	{
		if (sysmeta == null) {
			throw new InvalidSystemMetadata("1000",
					"System metadata was null.  Can't create multipart form.");
		}
		Date d = new Date();
		String boundary = d.getTime() + "";

		String mime = "MIME-Version:1.0\n";
		mime += "Content-type:multipart/mixed; boundary=\"" + boundary + "\"\n";
		boundary = "--" + boundary + "\n";
		mime += boundary;
		mime += "Content-Disposition: attachment; filename=systemmetadata\n\n";
		out.write(mime.getBytes());
		out.flush();
		
		//write the sys meta
		try
		{
		    IOUtils.copy(serializeSystemMetadata(sysmeta), out);
		}
		catch(Exception e)
		{
		    throw new ServiceFailure("1000",
                    "Could not serialize the system metadata to multipart: "
                            + e.getMessage());
		}
		
		out.write("\n".getBytes());	

		if (object != null) 
		{    
			out.write(boundary.getBytes());
			out.write("Content-Disposition: attachment; filename=object\n\n".getBytes());
			try {
				mime += IOUtils.copy(object, out);
			} 
			catch (IOException ioe) 
			{
				throw new ServiceFailure("1000",
						"Error serializing object to multipart: "
								+ ioe.getMessage());
			}
			out.write("\n".getBytes());
		}

		out.write((boundary + "--").getBytes());		
	}

	/**
	 * send a request to the resource and get the response
	 */
	protected ResponseData sendRequest(AuthToken token, String resource,
			String method, String urlParamaters, String contentType,
			InputStream dataStream, String acceptHeader) throws ServiceFailure {

		ResponseData resData = new ResponseData();
		HttpURLConnection connection = null;

		String restURL = nodeBaseServiceUrl + resource;

		if (urlParamaters != null) {
			if (restURL.indexOf("?") == -1)
				restURL += "?";
			restURL += urlParamaters;
			if (restURL.indexOf(" ") != -1) {
				restURL = restURL.replaceAll("\\s", "%20");
			}
		}

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
				System.out
						.println("tried to get content and failed.  getting error stream instead");
				content = connection.getErrorStream();
				// resData.setContentStream(content);
			}

			int code = connection.getResponseCode();
			resData.setHeaderFields(connection.getHeaderFields());
			resData.setCode(code);
			if (code != HttpURLConnection.HTTP_OK) {
				resData.setCode(code);
				resData.setErrorStream(connection.getErrorStream());
			}
		} catch (MalformedURLException e) {
			throw new ServiceFailure("1000", restURL + " " + e.getMessage());
		} catch (ProtocolException e) {
			throw new ServiceFailure("1000", restURL + " " + e.getMessage());
		} catch (FileNotFoundException e) {
			resData.setCode(404);
			resData.setErrorStream(connection.getErrorStream());
		} catch (IOException e) {
			e.printStackTrace();
			throw new ServiceFailure("1000", restURL + " " + e.getMessage());
		}

		return resData;
	}

    protected void deserializeAndThrowException(InputStream errorStream)
			throws NotFound, InvalidToken, ServiceFailure, NotAuthorized,
			NotFound, IdentifierNotUnique, UnsupportedType,
			InsufficientResources, InvalidSystemMetadata, NotImplemented,
			InvalidCredentials, InvalidRequest {

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		Document doc;

		try {
			DocumentBuilder db = dbf.newDocumentBuilder();
			doc = db.parse(errorStream);
			Element root = doc.getDocumentElement();
			root.normalize();
			int code = getIntAttribute(root, "errorCode");
			String detailCode = root.getAttribute("detailCode");
			String description = getTextValue(root, "description");
			switch (code) {
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
		} catch (SAXException e) {
            throw new ServiceFailure("1000",
            "Service failed, but error message not parsed correctly: " + e.getMessage());
		} catch (IOException e) {
            throw new ServiceFailure("1000",
                    "Service failed, but error message not parsed correctly: " + e.getMessage());
		} catch (ParserConfigurationException e) {
            throw new ServiceFailure("1000",
                    "Service failed, but error message not parsed correctly: " + e.getMessage());
		}
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
	protected int getIntAttribute(Element e, String attName) {
		String attText = e.getAttribute(attName);
		return Integer.parseInt(attText);
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
	
	/**
     * set action="create" for insert and action="update" for update
     */
    protected Identifier handleCreateOrUpdate(AuthToken token, Identifier guid,
        final InputStream object, final SystemMetadata sysmeta, Identifier obsoletedGuid, 
        String action)
        throws InvalidToken,ServiceFailure, NotAuthorized, IdentifierNotUnique,
            UnsupportedType, InsufficientResources, InvalidSystemMetadata,
            NotImplemented
    {
        String resource = Constants.RESOURCE_OBJECTS + "/" + EncodingUtilities.encodeUrlPathSegment(guid.getValue());

        File outputFile = null;
        InputStream multipartStream;
        try
        {
            Date d = new Date();
            File tmpDir = new File(Constants.TEMP_DIR);
            outputFile = new File(tmpDir, "mmp.output." + d.getTime());
            FileOutputStream dataSink = new FileOutputStream(outputFile);
            createMimeMultipart(dataSink, object, sysmeta);
            multipartStream = new FileInputStream(outputFile);
        }
        catch(Exception e)
        {
            outputFile.delete();
            throw new ServiceFailure("1000", 
                    "Error creating MMP stream in MNode.handleCreateOrUpdate: " + 
                    e.getMessage());
        }
        
        ResponseData rd = null;
        
        if(action.equals("create"))
        {
            rd = sendRequest(token, resource, Constants.POST, null,
                "multipart/mixed", multipartStream, null);
            InputStream responseStream = rd.getContentStream();
            
        }
        else if(action.equals("update"))
        {
            if(obsoletedGuid == null)
            {
                throw new NullPointerException("obsoletedGuid must not be null in MNode.update");
            }
            String urlParams = "obsoletedGuid=" + EncodingUtilities.encodeUrlQuerySegment(obsoletedGuid.getValue());
            rd = sendRequest(token, resource, Constants.PUT, urlParams,
                    "multipart/mixed", multipartStream, null);
        }
        
        InputStream is = rd.getContentStream();
        Identifier id;
        try 
        {
            id = (Identifier)deserializeServiceType(Identifier.class, is);
        } 
        catch (JiBXException e) 
        {
            throw new ServiceFailure("500",
                    "Could not deserialize the returned Identifier: " + e.getMessage());
        }
        finally
        {
            outputFile.delete();
        }

        // Handle any errors that were generated
        int code = rd.getCode();
        if (code != HttpURLConnection.HTTP_OK) {
            InputStream errorStream = rd.getErrorStream();
            try {
                byte[] b = new byte[1024];
                int numread = errorStream.read(b, 0, 1024);
                StringBuffer sb = new StringBuffer();
                while (numread != -1) {
                    sb.append(new String(b, 0, numread));
                    numread = errorStream.read(b, 0, 1024);
                }
                outputFile.delete();
                deserializeAndThrowException(errorStream);
            } catch (InvalidToken e) {
                outputFile.delete();
                throw e;
            } catch (ServiceFailure e) {
                outputFile.delete();
                throw e;
            } catch (NotAuthorized e) {
                outputFile.delete();
                throw e;
            } catch (IdentifierNotUnique e) {
                outputFile.delete();
                throw e;
            } catch (UnsupportedType e) {
                outputFile.delete();
                throw e;
            } catch (InsufficientResources e) {
                outputFile.delete();
                throw e;
            } catch (InvalidSystemMetadata e) {
                outputFile.delete();
                throw e;
            } catch (NotImplemented e) {
                outputFile.delete();
                throw e;
            } catch (BaseException e) {
                outputFile.delete();
                throw new ServiceFailure("1000",
                        "Method threw improper exception: " + e.getMessage());
            } catch (IOException e) {
                outputFile.delete();
                throw new ServiceFailure("1000",
                        "IOException in processing: " + e.getMessage());
            } finally {
                outputFile.delete();
            }
            
        } 
        outputFile.delete();
        return id;
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
