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
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpException;
import org.apache.http.client.ClientProtocolException;
import org.dataone.service.exceptions.AuthenticationTimeout;
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
import org.dataone.service.exceptions.UnsupportedMetadataType;
import org.dataone.service.exceptions.UnsupportedQueryType;
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.Constants;
import org.dataone.service.util.D1Url;
import org.dataone.service.util.TypeMarshaller;
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
    private String nodeId;
    protected boolean verbose = false;
    
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
        if (nodeBaseServiceUrl != null && !nodeBaseServiceUrl.endsWith("/")) {
            nodeBaseServiceUrl = nodeBaseServiceUrl + "/";
        }
        this.nodeBaseServiceUrl = nodeBaseServiceUrl;
    }
  
    /**
     * @return the nodeId
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * @param nodeId the nodeId to set
     */
    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public void setVerbose(boolean isVerbose){
    	this.verbose = isVerbose;
    }
    
    public boolean isVerbose() {
    	return this.verbose;
    }

    
    /**
     * creates a public session object that can be used as a default
     * session object if null is passed into a service api method. 
     * @return 
     */
    protected static Session createPublicSession() {

    	Session session = new Session();
    	Subject sub = new Subject();
    	sub.setValue("public");
    	session.setSubject(sub);
    	return session;
    }
    
  
    
	/**
     * Get the resource with the specified guid.  Used by both the CNode and 
     * MNode subclasses.
     * InputStream is the Java native version of D1's OctetStream
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.listObjects
     *
     */
    public InputStream get(Session cert, Identifier pid)
    throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, 
    NotImplemented, InvalidRequest 
    {
       	D1Url url = new D1Url(this.getNodeBaseServiceUrl(),Constants.RESOURCE_OBJECTS);
    	url.addNextPathElement(pid.getValue());
//       	if (cert != null)
//    		url.addNonEmptyParamPair("sessionid",token.getToken());

		D1RestClient client = new D1RestClient(true, verbose);
		
		InputStream is = null;
		try {
			is = client.doGetRequest(url.getUrl());
		} catch (IdentifierNotUnique e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidSystemMetadata e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidCredentials e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedMetadataType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidRequest e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (AuthenticationTimeout e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IllegalStateException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (HttpException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		}
		return is;
    }
 
	/**
     * Get the system metadata from a resource with the specified guid. Used
     * by both the CNode and MNode implementations.
     * @see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MN_read.getSystemMetadata
     */
	public SystemMetadata getSystemMetadata(Session session, Identifier pid)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
		InvalidRequest, NotImplemented 
	{
		
		D1Url url = new D1Url(this.getNodeBaseServiceUrl(),Constants.RESOURCE_META);
    	url.addNextPathElement(pid.getValue());
//       	if (cert != null)
//    		url.addNonEmptyParamPair("sessionid",cert.);

		D1RestClient client = new D1RestClient(true, verbose);
		
		InputStream is = null;
	
		try {
			is = client.doGetRequest(url.getUrl());		
		} catch (IdentifierNotUnique e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedQueryType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InsufficientResources e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidSystemMetadata e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (InvalidCredentials e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (AuthenticationTimeout e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (UnsupportedMetadataType e) {
			throw new ServiceFailure("0", "unexpected exception from the service - " + e.getClass() + ": "+ e.getMessage());
		} catch (ClientProtocolException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IllegalStateException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (HttpException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		}
		try {
            return deserializeServiceType(SystemMetadata.class,is);
        } catch (Exception e) {
            throw new ServiceFailure("1090",
                    "Could not deserialize the systemMetadata: " + e.getMessage());
        }	
	}
   
	

    /**
     * A helper function to preserve the stackTrace when catching one error and throwing a new one.
     * Also has some descriptive text which makes it clientSide specific
     * @param e
     * @return
     */
    protected static ServiceFailure recastClientSideExceptionToServiceFailure(Exception e) {
    	ServiceFailure sfe = new ServiceFailure("0 Client_Error", e.getClass() + ": "+ e.getMessage());
		sfe.setStackTrace(e.getStackTrace());
    	return sfe;
    }

    
    /**
     * A helper function for recasting DataONE exceptions to ServiceFailures while
     * preserving the detail code and TraceDetails.

     * @param be - BaseException subclass to be recast
     * @return ServiceFailure
     */
    protected static ServiceFailure recastDataONEExceptionToServiceFailure(BaseException be) {	
    	ServiceFailure sfe = new ServiceFailure(be.getDetail_code(), 
    			"Recasted unexpected exception from the service - " + be.getClass() + ": "+ be.getMessage());
    	
    	Iterator<String> it = be.getTraceKeySet().iterator();
    	while (it.hasNext()) {
    		String key = it.next();
    		sfe.addTraceDetail(key, be.getTraceDetail(key));
    	}
    	return sfe;
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
    protected static void deserializeAndThrowException(int httpCode, InputStream errorStream)
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
                        if (root.hasAttribute("detailCode")) {
                            detailCode = root.getAttribute("detailCode");
                        } else {
                            detailCode = "-1";
                        }
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
    private static String deserializeNonXMLErrorStream(BufferedInputStream errorStream, Exception e) 
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
	protected static String getTextValue(Element e, String tag) {
		String text = null;
		NodeList nl = e.getElementsByTagName(tag);
		if (nl != null && nl.getLength() > 0) {
			Element el = (Element) nl.item(0);
                        if ((el.getFirstChild().getNodeType() == Element.TEXT_NODE) ||
                                el.getFirstChild().getNodeType() == Element.CDATA_SECTION_NODE) {
                            text = el.getFirstChild().getNodeValue();
                        } else {
                            text = "";
                        }
		}
		return text;
	}

	/**
	 * return an xml attribute as an int
	 * @param e
	 * @param attName
	 * @return
	 */
	protected static int getIntAttribute(Element e, String attName)
	throws NumberFormatException {
            if (e.hasAttribute(attName)) {
		String attText = e.getAttribute(attName);
		int x = Integer.parseInt(attText);
		return x;
            } else {
                return -1;
            }
	}

//	/**
//	 * serialize an object of type to out
//	 * 
//	 * @param type
//	 *            the class of the object to serialize (i.e.
//	 *            SystemMetadata.class)
//	 * @param object
//	 *            the object to serialize
//	 * @param out
//	 *            the stream to serialize it to
//	 * @throws JiBXException
//	 */
//	@SuppressWarnings("rawtypes")
//	protected void serializeServiceType(Class type, Object object,
//			OutputStream out) throws JiBXException {
//		ServiceTypeUtil.serializeServiceType(type, object, out);
//	}

	/**
	 * deserialize an object of type from the inputstream
	 * This is a wrapper method of the standard common Unmarshalling method
	 * that recasts exceptions to ServiceFailure
	 * 
	 * @param type
	 *            the class of the object to serialize (i.e.
	 *            SystemMetadata.class)
	 * @param is
	 *            the stream to deserialize from
	 * @throws ServiceFailure 


	 */
	@SuppressWarnings("rawtypes")
	protected <T> T deserializeServiceType(Class<T> domainClass, InputStream is)
	throws ServiceFailure
	{
		try {
			return TypeMarshaller.unmarshalTypeFromStream(domainClass, is);
		} catch (JiBXException e) {
            throw new ServiceFailure("0",
                    "Could not deserialize the " + domainClass.getCanonicalName() + ": " + e.getMessage());
        } catch (IOException e) {
        	throw new ServiceFailure("0",
                    "Could not deserialize the " + domainClass.getCanonicalName() + ": " + e.getMessage());
		} catch (InstantiationException e) {
			throw new ServiceFailure("0",
                    "Could not deserialize the " + domainClass.getCanonicalName() + ": " + e.getMessage());
		} catch (IllegalAccessException e) {
			throw new ServiceFailure("0",
                    "Could not deserialize the " + domainClass.getCanonicalName() + ": " + e.getMessage());
		}
	}
}
