/**
 * Copyright 2010 Regents of the University of California and the
 *                National Center for Ecological Analysis and Synthesis
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package org.dataone.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.Date;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.mail.MessagingException;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
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
import org.dataone.service.mn.MemberNodeCrud;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Checksum;
import org.dataone.service.types.DescribeResponse;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.LogRecordSet;
import org.dataone.service.types.SystemMetadata;
import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IMarshallingContext;
import org.jibx.runtime.JiBXException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.gc.iotools.stream.is.InputStreamFromOutputStream;

/**
 * The D1Client class represents a client-side implementation of the DataONE
 * Service API.  The class exposes the DataONE APIs as client  methods,
 * dispatches the calls to the correct DataONE node, and then returns the
 * results or throws the appropriate exceptions.
 * 
 * @author Matthew Jones
 */
public class D1Client implements MemberNodeCrud {
    
    /** HTTP Verb GET*/
    public static final String GET = "GET";
    /** HTTP Verb POST*/    
    public static final String POST = "POST";
    /** HTTP Verb PUT*/
    public static final String PUT = "PUT";
    /** HTTP Verb DELETE*/
    public static final String DELETE = "DELETE";

    /*
     * API Resources
     */

    /** API OBJECTS Resource which handles with document operations*/
    public static final String RESOURCE_OBJECTS = "object";
    /** API SESSION Resource which handles with user session operations*/
    public static final String RESOURCE_SESSION = "session";
    /** API IDENTIFIER Resource which controls object unique identifier operations*/
    public static final String RESOURCE_IDENTIFIER = "identifier";
    
    /** The session identifier for the session */
    //private String sessionId;
    
    /** The URL string for the metacat REST API*/
    private String contextRootUrl;

    /**
     * Constructor to create a new instance. 
     */ 
    public D1Client(String contextRootUrl){
        this.contextRootUrl = contextRootUrl;
    }
    
    public Identifier create(AuthToken token, Identifier guid,
            InputStream object, SystemMetadata sysmeta) throws InvalidToken,
            ServiceFailure, NotAuthorized, IdentifierNotUnique,
            UnsupportedType, InsufficientResources, InvalidSystemMetadata,
            NotImplemented {
        
        String resource = RESOURCE_OBJECTS + "/" + guid.getValue();
        InputStream is = null;
        
        // Create a multipart message containing the data and sysmeta
        final MimeMultipart mmp = new MimeMultipart();
        try {
            MimeBodyPart objectPart = new MimeBodyPart();
            objectPart.addHeaderLine("Content-Transfer-Encoding: base64");
            objectPart.setFileName("object");
            DataSource ds = new InputStreamDataSource("object", object);
            DataHandler dh = new DataHandler(ds);
            objectPart.setDataHandler(dh);
            
            ByteArrayInputStream sysmetaStream = serializeSystemMetadata(sysmeta);
       
            MimeBodyPart sysmetaPart = new MimeBodyPart();
            sysmetaPart.addHeaderLine("Content-Transfer-Encoding: base64");
            sysmetaPart.setFileName("systemmetadata");
            DataSource smDs = new InputStreamDataSource("systemmetadata", sysmetaStream);
            DataHandler smDh = new DataHandler(smDs);
            sysmetaPart.setDataHandler(smDh);
            
            mmp.addBodyPart(sysmetaPart);
            mmp.addBodyPart(objectPart);
        } catch (MessagingException e) {
            throw new ServiceFailure(1190, "Failed constructing mime message on client create()...");
        } catch (JiBXException e) {
            e.printStackTrace(System.out);
            throw new InvalidSystemMetadata(1180, "Failed to marshal SystemMetadata.");
        }
        
        
        // write the mmp to an InputStream and pass it to SendRequest in last param
        final InputStreamFromOutputStream<String> multipartStream = 
            new InputStreamFromOutputStream<String>() {
            @Override
            public String produce(final OutputStream dataSink) throws Exception {
                mmp.writeTo(dataSink);
                IOUtils.closeQuietly(dataSink);
                return "Completed";
            }
        };
        ResponseData rd = sendRequest(resource, POST, null, 
                mmp.getContentType(), multipartStream);
        
        // Handle any errors that were generated
        int code = rd.getCode();
        if (code  != HttpURLConnection.HTTP_OK ) {
            InputStream errorStream = rd.getErrorStream();
            try {
                deserializeAndThrowException(errorStream);                
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
                throw new ServiceFailure(1000, 
                        "Method threw improper exception: " + e.getMessage());
            }
        } else {
            is = rd.getContentStream();
        }
        
        return guid;
    }

    public Identifier delete(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        throw new NotImplemented(1000, "Method not yet implemented.");
    }

    public DescribeResponse describe(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        throw new NotImplemented(1000, "Method not yet implemented.");
    }

    public InputStream get(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        String resource = RESOURCE_OBJECTS + "/" + guid.getValue();
        InputStream is = null;
        ResponseData rd = sendRequest(resource, GET, null, null, null);
        int code = rd.getCode();
        if (code  != HttpURLConnection.HTTP_OK ) {
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
                throw new ServiceFailure(1000, 
                        "Method threw improper exception: " + e.getMessage());
            }        
        } else {
            is = rd.getContentStream();
        }

        return is;
    }

    public Checksum getChecksum(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            InvalidRequest, NotImplemented {
        throw new NotImplemented(1000, "Method not yet implemented.");
    }

    public Checksum getChecksum(AuthToken token, Identifier guid,
            String checksumAlgorithm) throws InvalidToken, ServiceFailure,
            NotAuthorized, NotFound, InvalidRequest, NotImplemented {
        throw new NotImplemented(1000, "Method not yet implemented.");
    }

    public LogRecordSet getLogRecords(AuthToken token, Date fromDate,
            Date toDate) throws InvalidToken, ServiceFailure, NotAuthorized,
            InvalidRequest, NotImplemented {
        throw new NotImplemented(1000, "Method not yet implemented.");
    }

    public SystemMetadata getSystemMetadata(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            InvalidRequest, NotImplemented {
        throw new NotImplemented(1000, "Method not yet implemented.");
    }

    public Identifier update(AuthToken token, Identifier guid,
            InputStream object, Identifier obsoletedGuid,
            SystemMetadata sysmeta) throws InvalidToken, ServiceFailure,
            NotAuthorized, IdentifierNotUnique, UnsupportedType,
            InsufficientResources, NotFound, InvalidSystemMetadata,
            NotImplemented {
        throw new NotImplemented(1000, "Method not yet implemented.");
    }
    
    private ResponseData sendRequest(String resource, String method, 
            String urlParamaters, String contentType, InputStream dataStream) 
        throws ServiceFailure {
        
        ResponseData resData = new ResponseData();
        HttpURLConnection connection = null ;

        String restURL = contextRootUrl+resource;

        if (urlParamaters != null) {
            if (restURL.indexOf("?") == -1)             
                restURL += "?";
            restURL += urlParamaters; 
        }

        URL u = null;
        InputStream content = null;
        try {
            u = new URL(restURL);
            connection = (HttpURLConnection) u.openConnection();
            if (contentType!=null) {
                connection.setRequestProperty("Content-Type",contentType);
            }

            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setRequestMethod(method);
            
            if (!method.equals(GET)) {
                if (dataStream != null) {
                    OutputStream out = connection.getOutputStream();
                    IOUtils.copy(dataStream, out);
                }
            }
            
            content = connection.getInputStream();
            resData.setContentStream(content);
            int code = connection.getResponseCode();
            resData.setCode(code);
            if (code != HttpURLConnection.HTTP_OK) {
                resData.setCode(code);
                resData.setErrorStream(connection.getErrorStream());
            }
        } catch (MalformedURLException e) {
            throw new ServiceFailure(0, restURL + " " + e.getMessage());
        } catch (ProtocolException e) {
            throw new ServiceFailure(0, restURL + " " + e.getMessage());
        } catch (FileNotFoundException e) {
            resData.setCode(404);
            resData.setErrorStream(connection.getErrorStream());
        } catch (IOException e) {
            throw new ServiceFailure(0, restURL + " " + e.getMessage());
        }
        
        return resData;     
    }
    
    private void deserializeAndThrowException(InputStream errorStream) 
        throws NotFound, InvalidToken, ServiceFailure, NotAuthorized, 
        NotFound, IdentifierNotUnique, UnsupportedType, InsufficientResources, 
        InvalidSystemMetadata, NotImplemented, InvalidCredentials, InvalidRequest {
        BaseException b = null;
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        Document doc;
        boolean parseFailed = false;
        try {
            DocumentBuilder db = dbf.newDocumentBuilder();
            doc = db.parse(errorStream);
            Element root = doc.getDocumentElement();
            root.normalize();
            System.out.println(root.toString());
            int code = getIntAttribute(root, "errorCode");
            int detailCode = getIntAttribute(root, "detailCode");
            String description = getTextValue(root, "description");
            switch (code) {
            case 400:
                if (detailCode == 1180) {
                    throw new InvalidSystemMetadata(1180, description);
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
            parseFailed = true;
        } catch (IOException e) {
            parseFailed = true;
        } catch (ParserConfigurationException e) {
            parseFailed = true;
        }
        
        if (parseFailed) {
            throw new ServiceFailure(1000, 
                    "Service failed, but error message not parsed correctly.");
        }        
    }
    
    /**
     * Take a xml element and the tag name, return the text content of the child element.
     */
    private String getTextValue(Element e, String tag) {
        String text = null;
        NodeList nl = e.getElementsByTagName(tag);
        if(nl != null && nl.getLength() > 0) {
            Element el = (Element)nl.item(0);
            text = el.getFirstChild().getNodeValue();
        }
    
        return text;
    }

    private int getIntAttribute(Element e, String attName) {
        String attText = e.getAttribute(attName);
        return Integer.parseInt(attText);
    }

    private ByteArrayInputStream serializeSystemMetadata(SystemMetadata sysmeta)
            throws JiBXException {
        IBindingFactory bfact =
            BindingDirectory.getFactory(SystemMetadata.class);
        IMarshallingContext mctx = bfact.createMarshallingContext();
        ByteArrayOutputStream sysmetaOut = new ByteArrayOutputStream();
        mctx.marshalDocument(sysmeta, "UTF-8", null, sysmetaOut);
        ByteArrayInputStream sysmetaStream = 
            new ByteArrayInputStream(sysmetaOut.toByteArray());
        return sysmetaStream;
    }

    protected class ResponseData {
        private int code;
        private InputStream contentStream;
        private InputStream errorStream;
        
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
         * @param code the code to set
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
         * @param contentStream the contentStream to set
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
         * @param errorStream the errorStream to set
         */
        protected void setErrorStream(InputStream errorStream) {
            this.errorStream = errorStream;
        }
        
    }
}
