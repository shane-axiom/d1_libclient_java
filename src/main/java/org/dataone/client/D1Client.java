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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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
import org.dataone.service.mn.MemberNodeCrud;
import org.dataone.service.mn.MemberNodeReplication;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Checksum;
import org.dataone.service.types.DescribeResponse;
import org.dataone.service.types.Event;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.Log;
import org.dataone.service.types.ObjectFormat;
import org.dataone.service.types.ObjectList;
import org.dataone.service.types.SystemMetadata;
import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IMarshallingContext;
import org.jibx.runtime.IUnmarshallingContext;
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
public class D1Client implements MemberNodeCrud, MemberNodeReplication {
    
    // TODO: Need Javadocs throughout
    
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
    /** API META Resource which handles SystemMetadata operations*/
    public static final String RESOURCE_META = "meta";
    /** API SESSION Resource which handles with user session operations*/
    public static final String RESOURCE_SESSION = "session";
    /** API IDENTIFIER Resource which controls object unique identifier operations*/
    public static final String RESOURCE_IDENTIFIER = "identifier";
    /** API LOG  controls logging events*/
    public static final String RESOURCE_LOG = "log";
    
    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    
    /** The session identifier for the session */
    //private String sessionId;
    
    /** The URL string for the metacat REST API*/
    private String contextRootUrl;

    /**
     * Constructor to create a new instance. 
     */ 
    public D1Client(String contextRootUrl)
    {
        this.contextRootUrl = contextRootUrl;
    }
    
    public D1Client() {}
    
    /**
     * set the access perms for a document
     * @param token
     * @param id
     * @param principal
     * @param permission
     * @param permissionType
     * @param permissionOrder
     */
    public void setAccess(AuthToken token, Identifier id, String principal, String permission,
            String permissionType, String permissionOrder)
      throws ServiceFailure
    {
        String params = "guid=" + id.getValue() + "&principal=" + principal + 
          "&permission=" + permission + "&permissionType=" + permissionType +
          "&permissionOrder=" + permissionOrder +
          "&op=setaccess&setsystemmetadata=true";
        String resource = RESOURCE_SESSION + "/";
        ResponseData rd = sendRequest(token, resource, POST, params, null, null);
        int code = rd.getCode();
        if(code != HttpURLConnection.HTTP_OK)
        {
            throw new ServiceFailure("1000", "Error setting acces on document");
        }
        
        // TODO: also set the system metadata to the same perms
        
    }
    
    /**
     * login and get an AuthToken
     * @param username
     * @param password
     * @return
     * @throws ServiceFailure
     */
    public AuthToken login(String username, String password)
      throws ServiceFailure, NotImplemented
    {
        String postData = "username=" + username + "&password=" + password;
        String params = "qformat=xml&op=login";
        String resource = RESOURCE_SESSION + "/";
        
        ResponseData rd = sendRequest(null, resource, POST, params, null, 
            new ByteArrayInputStream(postData.getBytes()));
        String sessionid = null;
        
        int code = rd.getCode();
        if(code != HttpURLConnection.HTTP_OK)
        { //deal with the error
        	// TODO: detail codes are wrong
            throw new ServiceFailure("1000", "Error logging in.");
        }
        else
        {
            try
            {
            	// TODO: use IOUtils to get the string
                InputStream is = rd.getContentStream();
                byte[] b = new byte[1024];
                int numread = is.read(b, 0, 1024);
                StringBuffer sb = new StringBuffer();
                while(numread != -1)
                {
                    sb.append(new String(b, 0, numread));
                    numread = is.read(b, 0, 1024);
                }

                String response = sb.toString();
                int successIndex = response.indexOf("<sessionId>");
                if(successIndex != -1)
                {
                    sessionid = response.substring(
                            response.indexOf("<sessionId>") + "<sessionId>".length(), 
                            response.indexOf("</sessionId>"));
                }
                else
                {
                    throw new ServiceFailure("1000", "Error authenticating: " + 
                            response.substring(response.indexOf("<error>") + "<error>".length(), 
                                    response.indexOf("</error>")));
                }
            }
            catch(Exception e)
            {
                throw new ServiceFailure("1000", "Error getting response from metacat: " + e.getMessage());
            }
        }
        
        return new AuthToken(sessionid);
    }
    
    /**
     * list objects in the system
     * @param token
     * @param startTime
     * @param endTime
     * @param objectFormat
     * @param replicaStatus
     * @param start
     * @param count
     * @return
     * @throws NotAuthorized
     * @throws InvalidRequest
     * @throws NotImplemented
     * @throws ServiceFailure
     * @throws InvalidToken
     */
    @Override
    public ObjectList listObjects(AuthToken token, Date startTime, Date endTime, 
            ObjectFormat objectFormat, boolean replicaStatus, int start, 
            int count) throws NotAuthorized, InvalidRequest, NotImplemented, 
            ServiceFailure, InvalidToken
    {
        InputStream is = null;
        String resource = RESOURCE_OBJECTS;
        String params = "";
        
        if(startTime != null)
        {
            params += "startTime=" + convertDateToGMT(startTime);
        }
        
        if(endTime != null)
        {
            if(!params.equals(""))
            {
                params += "&";
            }
            
            params += "endTime=" + convertDateToGMT(endTime);
        }
        
        if(objectFormat != null)
        {
            if(!params.equals(""))
            {
                params += "&";
            }
            params += "objectFormat=" + objectFormat;
        }
        
        if(!params.equals(""))
        {
            params += "&";
        }
        params += "replicaStatus=" + replicaStatus;
        params += "&";
        params += "start=" + start;
        params += "&";
        params += "count=" + count;
                
        ResponseData rd = sendRequest(token, resource, GET, params, 
                null, null);
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
            } catch (NotImplemented e) {
                throw e;
            } catch (BaseException e) {
                throw new ServiceFailure("1000", 
                        "Method threw improper exception: " + e.getMessage());
            } 
            
        } else {
            is = rd.getContentStream();
        }
        
        try
        {
            return deserializeObjectList(is);
        }
        catch(Exception e)
        {
            throw new ServiceFailure("500", "Could not deserialize the ObjectList: " + e.getMessage());
        }
    }
    
    /**
     * convert a date to GMT
     * @param d
     * @return
     */
    private String convertDateToGMT(Date d)
    {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT-0"));
        String s = dateFormat.format(d);
        return s;
    }

    /**
     * create both a system metadata resource and science metadata resource with the specified guid
     */
    public Identifier create(AuthToken token, Identifier guid,
            InputStream object, SystemMetadata sysmeta) throws InvalidToken,
            ServiceFailure, NotAuthorized, IdentifierNotUnique,
            UnsupportedType, InsufficientResources, InvalidSystemMetadata,
            NotImplemented {

        String resource = RESOURCE_OBJECTS + "/" + guid.getValue();
        InputStream is = null;

        final String mmp = createMimeMultipart(object, sysmeta);
 
        final InputStreamFromOutputStream<String> multipartStream =
            new InputStreamFromOutputStream<String>() 
        {
            @Override
            public String produce(final OutputStream dataSink) throws Exception {
                //mmp.writeTo(dataSink);
                IOUtils.write(mmp.getBytes(), dataSink);
                IOUtils.closeQuietly(dataSink);
                return "Complete";
            }
        };

        
        ResponseData rd = sendRequest(token, resource, POST, null, 
                "multipart/mixed", multipartStream);
        
        // Handle any errors that were generated
        int code = rd.getCode();
        if (code  != HttpURLConnection.HTTP_OK ) {
            InputStream errorStream = rd.getErrorStream();
            try {
                byte[] b = new byte[1024];
                int numread = errorStream.read(b, 0, 1024);
                StringBuffer sb = new StringBuffer();
                while(numread != -1)
                {
                    sb.append(new String(b, 0, numread));
                    numread = errorStream.read(b, 0, 1024);
                }
                
                //System.out.println("ERROR: " + sb.toString());
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
                throw new ServiceFailure("1000", 
                        "Method threw improper exception: " + e.getMessage());
            } catch (IOException e) {
                System.out.println("io exception: " + e.getMessage());
            }
            
        } else {
            is = rd.getContentStream();
        }
        
        return guid;
    }
    
    /**
     * update a resource with the specified guid. 
     */
    public Identifier update(AuthToken token, Identifier guid,
            InputStream object, Identifier obsoletedGuid,
            SystemMetadata sysmeta) throws InvalidToken, ServiceFailure,
            NotAuthorized, IdentifierNotUnique, UnsupportedType,
            InsufficientResources, NotFound, InvalidSystemMetadata,
            NotImplemented {
        
        String resource = RESOURCE_OBJECTS + "/" + guid.getValue();
        InputStream is = null;
        
        // Create a multipart message containing the data and sysmeta
        final String mmp = createMimeMultipart(object, sysmeta);
        
        // write the mmp to an InputStream and pass it to SendRequest in last param
        final InputStreamFromOutputStream<String> multipartStream = 
            new InputStreamFromOutputStream<String>() {
            @Override
            public String produce(final OutputStream dataSink) throws Exception {
                //mmp.writeTo(dataSink);
                IOUtils.write(mmp.getBytes(), dataSink);
                IOUtils.closeQuietly(dataSink);
                return "Completed";
            }
        };
        
        String urlParams = "obsoletedGuid=" + obsoletedGuid.getValue();
        ResponseData rd = sendRequest(token, resource, PUT, urlParams, 
                "multipart/mixed", multipartStream);
        
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
                throw new ServiceFailure("1000", 
                        "Method threw improper exception: " + e.getMessage());
            }
            
        } else {
            is = rd.getContentStream();
        }
        
        return guid;
    }
    
    /**
     * get the system metadata from a resource with the specified guid.  NOT IMPLEMENTED.
     */
    public SystemMetadata getSystemMetadata(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            InvalidRequest, NotImplemented {
                
        String resource = RESOURCE_META + "/" + guid.getValue();
        InputStream is = null;
        ResponseData rd = sendRequest(token, resource, GET, null, null, null);
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
                throw new ServiceFailure("1000", 
                        "Method threw improper exception: " + e.getMessage());
            }        
        } else {
            is = rd.getContentStream();
        }
        
        try
        {
            return deserializeSystemMetadata(is);
        }
        catch(Exception e)
        {
            throw new ServiceFailure("1090", "Could not deserialize the systemMetadata: " + e.getMessage());
        }
    }

    /**
     * get the resource with the specified guid
     */
    public InputStream get(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        String resource = RESOURCE_OBJECTS + "/" + guid.getValue();
        InputStream is = null;
        ResponseData rd = sendRequest(token, resource, GET, null, null, null);
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
                throw new ServiceFailure("1000", 
                        "Method threw improper exception: " + e.getMessage());
            }        
        } else {
            is = rd.getContentStream();
        }

        return is;
    }
    
    /**
     * delete a resource with the specified guid.  NOT IMPLEMENTED.
     */
    public Identifier delete(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        throw new NotImplemented("1000", "Method not yet implemented.");
    }

    /**
     * describe a resource with the specified guid.  NOT IMPLEMENTED.
     */
    public DescribeResponse describe(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        throw new NotImplemented("1000", "Method not yet implemented.");
    }

    /**
     * get the checksum from a resource with the specified guid.  NOT IMPLEMENTED.
     */
    public Checksum getChecksum(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            InvalidRequest, NotImplemented {
        throw new NotImplemented("1000", "Method not yet implemented.");
    }

    /**
     * get the checksum from a resource with the specified guid.  NOT IMPLEMENTED.
     */
    public Checksum getChecksum(AuthToken token, Identifier guid,
            String checksumAlgorithm) throws InvalidToken, ServiceFailure,
            NotAuthorized, NotFound, InvalidRequest, NotImplemented {
        throw new NotImplemented("1000", "Method not yet implemented.");
    }

    /**
     * get the log records from a resource with the specified guid.
     */
    public Log getLogRecords(AuthToken token, Date fromDate,
            Date toDate, Event event) throws InvalidToken, ServiceFailure, 
            NotAuthorized, InvalidRequest, NotImplemented 
    {
        String resource = RESOURCE_LOG + "?";
        String params = null;
        if(fromDate != null)
        {
            //params = "fromDate=" + dateFormat.format(fromDate);
            params = "fromDate=" + convertDateToGMT(fromDate);
        }
        if(toDate != null)
        {
            if(params != null)
            {
                //params += "&toDate=" + dateFormat.format(toDate);
                params += "&toDate=" + convertDateToGMT(toDate);
            }
            else
            {
                //params = "toDate=" + dateFormat.format(toDate);
                params += "toDate=" + convertDateToGMT(toDate);
            }
        }
        if(event != null)
        {
            if(params != null)
            {
                params += "&event=" + event.toString();
            }
            else
            {
                params = "event=" + event.toString();
            }
        }
        
        InputStream is = null;
        ResponseData rd = sendRequest(token, resource, GET, params, null, null);
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
            } catch (NotImplemented e) {
                throw e;
            } catch (BaseException e) {
                throw new ServiceFailure("1000", 
                        "Method threw improper exception: " + e.getMessage());
            }        
        } else {
            is = rd.getContentStream();
        }
        
        try
        {
            return (Log)deserializeServiceType(Log.class, is);
        }
        catch(Exception e)
        {
            throw new ServiceFailure("1090", "Could not deserialize the Log: " + e.getMessage());
        }
        
    }
    
    /**
     * add ampersand to a param list if needed
     * @param params
     * @return
     */
    private String addAmp(String params)
    {
        if(params != null && !params.trim().equals(""))
        {
            params += "&";
        }
        return params;
    }
    
    /**
     * create a mime multipart message from object and sysmeta
     */
    private String createMimeMultipart(InputStream object, SystemMetadata sysmeta)
      throws ServiceFailure, InvalidSystemMetadata
    {
        if(sysmeta == null)
        {
            throw new InvalidSystemMetadata("1000", "System metadata was null.  Can't create multipart form.");
        }
        
        String sysmetaString = null;
        try
        {
            ByteArrayInputStream sysmetaStream = serializeSystemMetadata(sysmeta);
            sysmetaString = IOUtils.toString(sysmetaStream);
        }
        catch(Exception e)
        {
            throw new ServiceFailure("1000", "Could not serialize the system metadata: " + e.getMessage());
        }
        
        Date d = new Date();
        String boundary = d.getTime() + "";
        
        String mime = "MIME-Version:1.0\n";
        mime += "Content-type:multipart/mixed; boundary=\"" + boundary + "\"\n";
        boundary = "--" + boundary + "\n";
        mime += boundary;
        mime += "Content-Disposition: attachment; filename=systemmetadata\n\n";
        mime += sysmetaString;
        mime += "\n";
        
        if(object != null)
        {
            mime += boundary;
            mime += "Content-Disposition: attachment; filename=object\n\n";
            try
            {
              mime += IOUtils.toString(object);
            }
            catch(IOException ioe)
            {
                throw new ServiceFailure("1000", "Error serializing object to multipart form: " + ioe.getMessage());
            }
            mime += "\n";
        }
        
        mime += boundary + "--";
        return mime;
    }
    
    private String streamToString(InputStream is)
    throws Exception
    {
        byte b[] = new byte[1024];
        int numread = is.read(b, 0, 1024);
        String response = new String();
        while(numread != -1)
        {
            response += new String(b, 0, numread);
            numread = is.read(b, 0, 1024);
        }
        return response;
    }

    private InputStream stringToStream(String s)
    throws Exception
    {
        ByteArrayInputStream bais = new ByteArrayInputStream(s.getBytes());
        return bais;
    }
    
    /**
     * send a request to the resource
     */
    private ResponseData sendRequest(AuthToken token, String resource, String method, 
            String urlParamaters, String contentType, InputStream dataStream) 
        throws ServiceFailure {
        
        ResponseData resData = new ResponseData();
        HttpURLConnection connection = null ;

        String restURL = contextRootUrl+resource;

        if (urlParamaters != null) {
            if (restURL.indexOf("?") == -1)             
                restURL += "?";
            restURL += urlParamaters; 
            if(restURL.indexOf(" ") != -1)
            {
                restURL = restURL.replaceAll("\\s", "%20");
            }
        }
        
        if(token != null)
        {
            if(restURL.indexOf("?") == -1)
            {
                restURL += "?sessionid=" + token.getToken();
            }
            else
            {
                restURL += "&sessionid=" + token.getToken();
            }
        }

        URL u = null;
        InputStream content = null;
        try {
            
            if(restURL.indexOf("+") != -1)
            {
                restURL = restURL.replaceAll("\\+", "%2b");
            }
            System.out.println("restURL: " + restURL);
            System.out.println("method: " + method);
            
            u = new URL(restURL);
            connection = (HttpURLConnection) u.openConnection();
            if (contentType!=null) {
                connection.setRequestProperty("Content-Type",contentType);
            }

            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setRequestMethod(method);
            connection.connect();
            
            if (!method.equals(GET)) {
                if (dataStream != null) {
                    OutputStream out = connection.getOutputStream();
                    IOUtils.copy(dataStream, out);
                }
            }
            
            try
            {
                content = connection.getInputStream();
                resData.setContentStream(content);
            }
            catch(IOException ioe)
            {
                System.out.println("tried to get content and failed.  getting error stream instead");
                content = connection.getErrorStream();
                //resData.setContentStream(content);
            }
            
            int code = connection.getResponseCode();
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
            int code = getIntAttribute(root, "errorCode");
            String detailCode = root.getAttribute("detailCode");
            String description = getTextValue(root, "description");
            switch (code) {
            case 400:
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
            parseFailed = true;
        } catch (IOException e) {
            parseFailed = true;
        } catch (ParserConfigurationException e) {
            parseFailed = true;
        }
        
        if (parseFailed) {
            throw new ServiceFailure("1000", 
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
        
        /*IBindingFactory bfact =
            BindingDirectory.getFactory(SystemMetadata.class);
        IMarshallingContext mctx = bfact.createMarshallingContext();
        ByteArrayOutputStream sysmetaOut = new ByteArrayOutputStream();
        mctx.marshalDocument(sysmeta, "UTF-8", null, sysmetaOut);
        ByteArrayInputStream sysmetaStream = 
            new ByteArrayInputStream(sysmetaOut.toByteArray());
        return sysmetaStream;*/

        ByteArrayOutputStream sysmetaOut = new ByteArrayOutputStream();
        serializeServiceType(SystemMetadata.class, sysmeta, sysmetaOut);
        ByteArrayInputStream sysmetaStream = 
            new ByteArrayInputStream(sysmetaOut.toByteArray());
        return sysmetaStream;
    }
    
    private SystemMetadata deserializeSystemMetadata(InputStream is)
      throws JiBXException
    {
        return (SystemMetadata)deserializeServiceType(SystemMetadata.class, is);
    }
    
    private ObjectList deserializeObjectList(InputStream is)
      throws JiBXException
    {
        return (ObjectList)deserializeServiceType(ObjectList.class, is);
    }

    /**
     * serialize an object of type to out
     * @param type the class of the object to serialize (i.e. SystemMetadata.class)
     * @param object the object to serialize
     * @param out the stream to serialize it to
     * @throws JiBXException
     */
    private void serializeServiceType(Class type, Object object, OutputStream out)
      throws JiBXException
    {
        IBindingFactory bfact = BindingDirectory.getFactory(type);
        IMarshallingContext mctx = bfact.createMarshallingContext();
        mctx.marshalDocument(object, "UTF-8", null, out);
    }
    
    /**
     * deserialize an object of type from is
     * @param type the class of the object to serialize (i.e. SystemMetadata.class)
     * @param is the stream to deserialize from
     * @throws JiBXException
     */
    private Object deserializeServiceType(Class type, InputStream is)
      throws JiBXException
    {
        IBindingFactory bfact = BindingDirectory.getFactory(type);
        IUnmarshallingContext uctx = bfact.createUnmarshallingContext();
        Object o = (Object) uctx.unmarshalDocument(is, null);
        return o;
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

    public String getContextRootUrl() {
        return contextRootUrl;
    }

    public void setContextRootUrl(String contextRootUrl) {
        this.contextRootUrl = contextRootUrl;
    }
    
}
