package org.dataone.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InsufficientResources;
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

import com.gc.iotools.stream.is.InputStreamFromOutputStream;

/**
 * MNode represents a MemberNode, and exposes the services associated with a
 * DataONE Member Node, allowing calling clients to call the services associated
 * with the node.
 */
public class MNode extends D1Node implements MemberNodeCrud, MemberNodeReplication {

    /**
     * Construct a Member Node, passing in the base url for node services.
     * @param nodeBaseServiceUrl base url for constructing service endpoints.
     */
    public MNode(String nodeBaseServiceUrl) {
        super(nodeBaseServiceUrl);
    }
    
    /**
     * set the access perms for a document
     * 
     * @param token
     * @param id
     * @param principal
     * @param permission
     * @param permissionType
     * @param permissionOrder
     */
    public void setAccess(AuthToken token, Identifier id, String principal,
            String permission, String permissionType, String permissionOrder)
            throws ServiceFailure {
        // TODO: this method assumes an access control model that is not finalized, refactor when it is
        String params = "guid=" + id.getValue() + "&principal=" + principal
                + "&permission=" + permission + "&permissionType="
                + permissionType + "&permissionOrder=" + permissionOrder
                + "&op=setaccess&setsystemmetadata=true";
        String resource = RESOURCE_SESSION + "/";
        ResponseData rd = sendRequest(token, resource, POST, params, null, null);
        int code = rd.getCode();
        if (code != HttpURLConnection.HTTP_OK) {
            throw new ServiceFailure("1000", "Error setting acces on document");
        }

        // TODO: also set the system metadata to the same perms

    }

    /**
     * login and get an AuthToken
     * 
     * @param username
     * @param password
     * @return
     * @throws ServiceFailure
     */
    public AuthToken login(String username, String password)
            throws ServiceFailure, NotImplemented {
        // TODO: reassess the exceptions thrown here.  Look at the Authentication interface.
        // TODO: this method assumes an access control model that is not finalized, refactor when it is
        String postData = "username=" + username + "&password=" + password;
        String params = "qformat=xml&op=login";
        String resource = RESOURCE_SESSION + "/";

        ResponseData rd = sendRequest(null, resource, POST, params, null,
                new ByteArrayInputStream(postData.getBytes()));
        String sessionid = null;

        int code = rd.getCode();
        if (code != HttpURLConnection.HTTP_OK) { // deal with the error
            // TODO: detail codes are wrong, and exception is the wrong one too I think
            throw new ServiceFailure("1000", "Error logging in.");
        } else {
            try {
                // TODO: use IOUtils to get the string, as this code is error prone
                InputStream is = rd.getContentStream();
                byte[] b = new byte[1024];
                int numread = is.read(b, 0, 1024);
                StringBuffer sb = new StringBuffer();
                while (numread != -1) {
                    sb.append(new String(b, 0, numread));
                    numread = is.read(b, 0, 1024);
                }
                String response = sb.toString();
                //String response = IOUtils.toString(is);

                
                int successIndex = response.indexOf("<sessionId>");
                if (successIndex != -1) {
                    sessionid = response.substring(
                            response.indexOf("<sessionId>")
                                    + "<sessionId>".length(),
                            response.indexOf("</sessionId>"));
                } else {
                    // TODO: wrong exception thrown, wrong detail code?
                    throw new ServiceFailure("1000", "Error authenticating: "
                            + response.substring(response.indexOf("<error>")
                                    + "<error>".length(),
                                    response.indexOf("</error>")));
                }
            } catch (Exception e) {
                throw new ServiceFailure("1000",
                        "Error getting response from metacat: "
                                + e.getMessage());
            }
        }

        return new AuthToken(sessionid);
    }

    /**
     * list objects in the system
     * 
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
    public ObjectList listObjects(AuthToken token, Date startTime,
            Date endTime, ObjectFormat objectFormat, boolean replicaStatus,
            int start, int count) throws NotAuthorized, InvalidRequest,
            NotImplemented, ServiceFailure, InvalidToken {
        InputStream is = null;
        String resource = RESOURCE_OBJECTS;
        String params = "";

        if (startTime != null) {
            params += "startTime=" + convertDateToGMT(startTime);
        }
        
        // TODO: should check that endTime >= startTime, throw InvalidRequest if not
        if (endTime != null) {
            if (!params.equals("")) {
                params += "&";
            }

            params += "endTime=" + convertDateToGMT(endTime);
        }

        // TODO: Check that the format is valid, throw InvalidRequest if not
        if (objectFormat != null) {
            if (!params.equals("")) {
                params += "&";
            }
            params += "objectFormat=" + objectFormat;
        }

        if (!params.equals("")) {
            params += "&";
        }
        
        // TODO: what if these are null?  Safely ignored?
        params += "replicaStatus=" + replicaStatus;
        params += "&";
        params += "start=" + start;
        params += "&";
        params += "count=" + count;

        ResponseData rd = sendRequest(token, resource, GET, params, null, null);
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
            } catch (NotImplemented e) {
                throw e;
            } catch (BaseException e) {
                throw new ServiceFailure("1000",
                        "Method threw improper exception: " + e.getMessage());
            }

        } else {
            is = rd.getContentStream();
        }
        
        // TODO: this block should be inside the preceding conditional I think
        try {
            return deserializeObjectList(is);
            
        // TODO: never catch an Exception per se -- it masks bad behavior
        } catch (Exception e) {
            throw new ServiceFailure("500",
                    "Could not deserialize the ObjectList: " + e.getMessage());
        }
    }

    /**
     * create both a system metadata resource and science metadata resource with
     * the specified guid
     */
    public Identifier create(AuthToken token, Identifier guid,
            InputStream object, SystemMetadata sysmeta) throws InvalidToken,
            ServiceFailure, NotAuthorized, IdentifierNotUnique,
            UnsupportedType, InsufficientResources, InvalidSystemMetadata,
            NotImplemented {

        String resource = RESOURCE_OBJECTS + "/" + guid.getValue();
        // TODO: This input stream is assigned below but not used.
        InputStream is = null;

        final String mmp = createMimeMultipart(object, sysmeta);

        final InputStreamFromOutputStream<String> multipartStream = new InputStreamFromOutputStream<String>() {
            @Override
            public String produce(final OutputStream dataSink) throws Exception {
                // mmp.writeTo(dataSink);
                // TODO: this appears memory bound and therefore not scalable; avoid getBytes()
                IOUtils.write(mmp.getBytes(), dataSink);
                IOUtils.closeQuietly(dataSink);
                return "Complete";
            }
        };

        ResponseData rd = sendRequest(token, resource, POST, null,
                "multipart/mixed", multipartStream);

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
            
        // TODO: Unclear why the conditional below exists; need to refactor;
        // probably is meant to check the return value to make sure the guid matches
        } else {
            is = rd.getContentStream();
        }

        return guid;
    }

    /**
     * update a resource with the specified guid.
     */
    public Identifier update(AuthToken token, Identifier guid,
            InputStream object, Identifier obsoletedGuid, SystemMetadata sysmeta)
            throws InvalidToken, ServiceFailure, NotAuthorized,
            IdentifierNotUnique, UnsupportedType, InsufficientResources,
            NotFound, InvalidSystemMetadata, NotImplemented {

        String resource = RESOURCE_OBJECTS + "/" + guid.getValue();
        // TODO: This input stream is assigned below but not used.
        InputStream is = null;

        // TODO: Much of the code in this method is a direct copy of the code in
        // insert() above -- factor out duplication

        // Create a multipart message containing the data and sysmeta
        final String mmp = createMimeMultipart(object, sysmeta);

        // write the mmp to an InputStream and pass it to SendRequest in last param
        final InputStreamFromOutputStream<String> multipartStream = new InputStreamFromOutputStream<String>() {
            @Override
            public String produce(final OutputStream dataSink) throws Exception {
                // mmp.writeTo(dataSink);
                // TODO: this appears memory bound and therefore not scalable; avoid getBytes()
                IOUtils.write(mmp.getBytes(), dataSink);
                IOUtils.closeQuietly(dataSink);
                return "Completed";
            }
        };

        // TODO: what if obsoletedGuid is null? Safe?
        String urlParams = "obsoletedGuid=" + obsoletedGuid.getValue();
        ResponseData rd = sendRequest(token, resource, PUT, urlParams,
                "multipart/mixed", multipartStream);

        // Handle any errors that were generated
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
            
        // TODO: Unclear why the conditional below exists; need to refactor;
        // probably is meant to check the return value to make sure the guid matches
        } else {
            is = rd.getContentStream();
        }

        return guid;
    }

    /**
     * get the system metadata from a resource with the specified guid.
     */
    public SystemMetadata getSystemMetadata(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            InvalidRequest, NotImplemented {

        String resource = RESOURCE_META + "/" + guid.getValue();
        InputStream is = null;
        ResponseData rd = sendRequest(token, resource, GET, null, null, null);
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
     * get the resource with the specified guid
     */
    public InputStream get(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        String resource = RESOURCE_OBJECTS + "/" + guid.getValue();
        InputStream is = null;
        ResponseData rd = sendRequest(token, resource, GET, null, null, null);
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
     * delete a resource with the specified guid. NOT IMPLEMENTED.
     */
    public Identifier delete(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        // TODO: Implement client delete method
        throw new NotImplemented("1000", "Method not yet implemented.");
    }

    /**
     * describe a resource with the specified guid. NOT IMPLEMENTED.
     */
    public DescribeResponse describe(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        // TODO: Implement client describe method
        throw new NotImplemented("1000", "Method not yet implemented.");
    }

    /**
     * get the checksum from a resource with the specified guid. NOT
     * IMPLEMENTED.
     */
    public Checksum getChecksum(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            InvalidRequest, NotImplemented {
        // TODO: Implement client getChecksum method
        throw new NotImplemented("1000", "Method not yet implemented.");
    }

    /**
     * get the checksum from a resource with the specified guid. NOT
     * IMPLEMENTED.
     */
    public Checksum getChecksum(AuthToken token, Identifier guid,
            String checksumAlgorithm) throws InvalidToken, ServiceFailure,
            NotAuthorized, NotFound, InvalidRequest, NotImplemented {
        // TODO: Implement client getChecksum method
        throw new NotImplemented("1000", "Method not yet implemented.");
    }

    /**
     * get the log records from a resource with the specified guid.
     */
    public Log getLogRecords(AuthToken token, Date fromDate, Date toDate,
            Event event) throws InvalidToken, ServiceFailure, NotAuthorized,
            InvalidRequest, NotImplemented {
        String resource = RESOURCE_LOG + "?";
        String params = null;
        if (fromDate != null) {
            params = "fromDate=" + convertDateToGMT(fromDate);
        }
        if (toDate != null) {
            if (params != null) {
                params += "&toDate=" + convertDateToGMT(toDate);
            } else {
                params += "toDate=" + convertDateToGMT(toDate);
            }
        }
        if (event != null) {
            if (params != null) {
                params += "&event=" + event.toString();
            } else {
                params = "event=" + event.toString();
            }
        }

        InputStream is = null;
        ResponseData rd = sendRequest(token, resource, GET, params, null, null);
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
            } catch (NotImplemented e) {
                throw e;
            } catch (BaseException e) {
                throw new ServiceFailure("1000",
                        "Method threw improper exception: " + e.getMessage());
            }
        } else {
            is = rd.getContentStream();
        }

        try {
            return (Log) deserializeServiceType(Log.class, is);
        } catch (Exception e) {
            throw new ServiceFailure("1090", "Could not deserialize the Log: "
                    + e.getMessage());
        }

    }

}
