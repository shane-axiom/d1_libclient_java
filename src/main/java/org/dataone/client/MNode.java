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
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
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
import org.dataone.service.mn.MemberNodeCrud;
import org.dataone.service.mn.MemberNodeReplication;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Checksum;
import org.dataone.service.types.ChecksumAlgorithm;
import org.dataone.service.types.DescribeResponse;
import org.dataone.service.types.Event;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.Log;
import org.dataone.service.types.ObjectFormat;
import org.dataone.service.types.ObjectList;
import org.dataone.service.types.SystemMetadata;
import org.jibx.runtime.JiBXException;

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
        String params = "guid=" + EncodingUtilities.encodeUrlQuerySegment(id.getValue()) + "&principal=" + principal
                + "&permission=" + permission + "&permissionType="
                + permissionType + "&permissionOrder=" + permissionOrder
                + "&op=setaccess&setsystemmetadata=true";
        String resource = Constants.RESOURCE_SESSION + "/";
        ResponseData rd = sendRequest(token, resource, Constants.POST, params, null, null, null);
        int code = rd.getCode();
        if (code != HttpURLConnection.HTTP_OK) {
            throw new ServiceFailure("1000", "Error setting acces on document");
        }
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
        String resource = Constants.RESOURCE_SESSION + "/";

        ResponseData rd = sendRequest(null, resource, Constants.POST, params, null,
                new ByteArrayInputStream(postData.getBytes()), null);
        String sessionid = null;

        int code = rd.getCode();
        if (code != HttpURLConnection.HTTP_OK) { // deal with the error
            // TODO: detail codes are wrong, and exception is the wrong one too I think
            throw new ServiceFailure("1000", "Error logging in.");
        } else {
            try {
                InputStream is = rd.getContentStream();
                String response = IOUtils.toString(is);
                
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

	  return super.listObjects(token, startTime, endTime, objectFormat, 
			  new Boolean(replicaStatus), new Integer(start), new Integer(count));
  }
    
    
    
//    @Override
//    public ObjectList listObjects(AuthToken token, Date startTime,
//            Date endTime, ObjectFormat objectFormat, boolean replicaStatus,
//            int start, int count) throws NotAuthorized, InvalidRequest,
//            NotImplemented, ServiceFailure, InvalidToken {
//        InputStream is = null;
//        String resource = Constants.RESOURCE_OBJECTS;
//        String params = "";
//
//        if (startTime != null) {
//            params += "startTime=" + convertDateToGMT(startTime);
//        }
//        
//        if (endTime != null && startTime != null && !endTime.after(startTime))
//        {
//            throw new InvalidRequest("1000", "startTime must be after stopTime in NMode.listObjects");
//        }
//        
//        if (endTime != null) {
//            if (!params.equals("")) {
//                params += "&";
//            }
//
//            params += "endTime=" + convertDateToGMT(endTime);
//        }
//
//        // TODO: Check that the format is valid, throw InvalidRequest if not
//        if (objectFormat != null) {
//            if (!params.equals("")) {
//                params += "&";
//            }
//            params += "objectFormat=" + objectFormat;
//        }
//
//        if (!params.equals("")) {
//            params += "&";
//        }
//        
//        params += "replicaStatus=" + replicaStatus;
//        params += "&";
//        params += "start=" + start;
//        params += "&";
//        params += "count=" + count;
//
//        ResponseData rd = sendRequest(token, resource, Constants.GET, params, null, null, null);
//        int code = rd.getCode();
//        if (code != HttpURLConnection.HTTP_OK) {
//            InputStream errorStream = rd.getErrorStream();
//            try {
//                deserializeAndThrowException(code,errorStream);
//            } catch (InvalidToken e) {
//                throw e;
//            } catch (ServiceFailure e) {
//                throw e;
//            } catch (NotAuthorized e) {
//                throw e;
//            } catch (NotImplemented e) {
//                throw e;
//            } catch (BaseException e) {
//                throw new ServiceFailure("1000",
//                        "Method threw improper exception: " + e.getMessage());
//            }
//
//        } else {
//            is = rd.getContentStream();
//        }
//        
//        try {
//            return deserializeObjectList(is);
//        } catch (JiBXException e) {
//            throw new ServiceFailure("500",
//                    "Could not deserialize the ObjectList: " + e.getMessage());
//        }
//    }

    /**
     * create both a system metadata resource and science metadata resource with
     * the specified guid
     */
    public Identifier create(AuthToken token, Identifier guid,
            InputStream object, SystemMetadata sysmeta) throws InvalidToken,
            ServiceFailure, NotAuthorized, IdentifierNotUnique,
            UnsupportedType, InsufficientResources, InvalidSystemMetadata,
            NotImplemented {
                
        return handleCreateOrUpdate(token, guid, object, sysmeta, null, "create");
    }

    public Identifier create2(AuthToken token, Identifier guid,
            InputStream object, SystemMetadata sysmeta) 
    throws InvalidToken,ServiceFailure, NotAuthorized, IdentifierNotUnique,
            UnsupportedType, InsufficientResources, InvalidSystemMetadata,
            NotImplemented 
    {
    	Identifier id = null;
    	
    	String restURL = getNodeBaseServiceUrl() + Constants.RESOURCE_OBJECTS;

    	MultipartRequestHandler mmpHandler = new MultipartRequestHandler(restURL,Constants.POST);
    	
    	
//		mmpHandler.addParamPart("sysmeta", sysmeta.toString());
		
		ByteArrayInputStream is;
		try {
			is = serializeSystemMetadata(sysmeta);
		} catch (JiBXException e) {
			throw new ServiceFailure("1000","Error serializing system metadata object: " + e.getMessage());
		}
		mmpHandler.addFilePart(is, "systemmetadata");
		mmpHandler.addFilePart(object, "object");
		mmpHandler.addParamPart("pid",guid.getValue());

		HttpResponse res = null;
		try {
			res = mmpHandler.executeRequest();
		} catch (ClientProtocolException e) {
			throw new ServiceFailure("1000",e.getClass() + " ERROR:" + e.getMessage());
		} catch (IOException e) {
			throw new ServiceFailure("1000",e.getClass() + " ERROR:" + e.getMessage());
		}
    	
		int code = res.getStatusLine().getStatusCode();
	
		if (code != HttpURLConnection.HTTP_OK) {
			// error, so throw exception
			try {
				deserializeAndThrowException(code,res.getEntity().getContent());
			} catch (NotFound e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidCredentials e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidRequest e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalStateException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// TODO: figure out how to throw the correct error!!!!!
//			throw new ServiceFailure("inadequate error typing",restURL);
		}
		
		InputStream content = null;
		try {
			if (res.getEntity() != null && res.getEntity().getContent() != null)
				content = res.getEntity().getContent();
			String echoed = IOUtils.toString(content);
			System.out.println("Echoed content:");
			System.out.println(echoed);
//		} catch (IllegalStateException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try	{
			if (content != null)
				id = (Identifier)deserializeServiceType(Identifier.class, content);
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
    
    /**
     * update a resource with the specified guid.
     */
    public Identifier update(AuthToken token, Identifier guid,
            InputStream object, Identifier obsoletedGuid, SystemMetadata sysmeta)
            throws InvalidToken, ServiceFailure, NotAuthorized,
            IdentifierNotUnique, UnsupportedType, InsufficientResources,
            NotFound, InvalidSystemMetadata, NotImplemented {

        return handleCreateOrUpdate(token, guid, object, sysmeta, obsoletedGuid, "update");
    }

    @Override
    public InputStream get(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented {
        return super.get(token, guid);
    }

    @Override
    public SystemMetadata getSystemMetadata(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            InvalidRequest, NotImplemented {
        return super.getSystemMetadata(token, guid);
    }

    /**
     * delete a resource with the specified guid. NOT IMPLEMENTED.
     */
    public Identifier delete(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented, InvalidRequest
    {
        String resource = Constants.RESOURCE_OBJECTS + "/" + EncodingUtilities.encodeUrlPathSegment(guid.getValue());
        
        if(token == null)
        {
            token = new AuthToken("public");
        }
        if(guid == null || guid.getValue().trim().equals(""))
        {
            throw new InvalidRequest("1322", "GUID cannot be null.");
        }
        
        ResponseData rd = sendRequest(token, resource, Constants.DELETE, null, null, null, null);
        InputStream is = rd.getContentStream();
        int code = rd.getCode();
        if (code != HttpURLConnection.HTTP_OK) 
        {
            InputStream errorStream = rd.getErrorStream();
            try {
                deserializeAndThrowException(code,errorStream);
            } catch (InvalidToken e) {
                throw e;
            } catch (ServiceFailure e) {
                throw e;
            } catch (NotAuthorized e) {
                throw e;
            } catch (NotImplemented e) {
                throw e;
            }  catch(InvalidRequest ir) {
                throw ir;
            } catch (BaseException e) {
                throw new ServiceFailure("1000",
                        "Method threw improper exception: " + e.getMessage());
            }
            

        } 
        else 
        {
            is = rd.getContentStream();
            try 
            {
                return (Identifier) deserializeServiceType(Identifier.class, is);
            } 
            catch (JiBXException e) 
            {
                throw new ServiceFailure("500",
                        "Could not deserialize the returned Identifier: " + e.getMessage());
            }
        }
        return null;
    }

    /**
     * describe a resource with the specified guid. NOT IMPLEMENTED.
     */
    public DescribeResponse describe(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            NotImplemented, InvalidRequest
    {
        String resource = Constants.RESOURCE_OBJECTS + "/" + EncodingUtilities.encodeUrlPathSegment(guid.getValue());
        String params = "";
        if(token == null)
        {
            token = new AuthToken("public");
        }
        if(guid == null || guid.getValue().trim().equals(""))
        {
            throw new InvalidRequest("1362", "GUID cannot be null.");
        }
        
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        params += "token=" + token.getToken();
        ResponseData rd = sendRequest(token, resource, Constants.HEAD, params, null, null, null);
        Map<String, List<String>> m = rd.getHeaderFields();
        String formatStr = m.get("format").get(0);
        String last_modifiedStr = m.get("last_modified").get(0);
        String content_lengthStr = m.get("content_length").get(0);
        String checksumStr = m.get("checksum").get(0);
        String checksum_algorithmStr = m.get("checksum_algorithm").get(0);
        ObjectFormat format = ObjectFormat.convert(formatStr);
        long content_length = new Long(content_lengthStr).longValue();
        Date last_modified = null;
        System.out.println("parsing date");
        try
        {
            last_modified = dateFormat.parse(last_modifiedStr.trim());
            /*Date d = new Date();
            dateFormat.setLenient(false);
            String dStr = dateFormat.format(d);
            System.out.println("d: " + d);
            System.out.println("dStr: " + dStr);
            Date e = dateFormat.parse(dStr);
            System.out.println("e: " + e);*/
        }
        catch(java.text.ParseException pe)
        {
            throw new InvalidRequest("1362", "Could not parse the date string " + 
                    last_modifiedStr + ". It should be in the format 'yyyy-MM-dd'T'hh:mm:ss.SZ': " +
                    pe.getMessage());
        }
        Checksum checksum = new Checksum();
        checksum.setAlgorithm(ChecksumAlgorithm.convert(checksum_algorithmStr));
        checksum.setValue(checksumStr);
        
        DescribeResponse dr = new DescribeResponse(format, content_length, last_modified, checksum);
        return dr;
    }

    /**
     * get the checksum from a resource with the specified guid. NOT
     * IMPLEMENTED.
     */
    public Checksum getChecksum(AuthToken token, Identifier guid)
            throws InvalidToken, ServiceFailure, NotAuthorized, NotFound,
            InvalidRequest, NotImplemented {
        return getChecksum(token, guid, null);
    }

    /**
     * get the checksum from a resource with the specified guid. NOT
     * IMPLEMENTED.
     */
    public Checksum getChecksum(AuthToken token, Identifier guid,
            String checksumAlgorithm) throws InvalidToken, ServiceFailure,
            NotAuthorized, NotFound, InvalidRequest, NotImplemented 
    {
        String resource = Constants.RESOURCE_CHECKSUM;
        String params = "";
        if(token == null)
        {
            token = new AuthToken("public");
        }
        if(guid == null || guid.getValue().trim().equals(""))
        {
            throw new InvalidRequest("1402", "GUID cannot be null.");
        }
        
        params += "token=" + token.getToken() + "&id=" + EncodingUtilities.encodeUrlQuerySegment(guid.getValue());
        
        if(!checksumAlgorithm.trim().equals("") && checksumAlgorithm != null)
        {
            params += "&checksumAlgorithm=" + checksumAlgorithm;
        }
        
        InputStream is = null;
        ResponseData rd = sendRequest(token, resource, Constants.GET, params, null, null, null);
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
            } catch (NotImplemented e) {
                throw e;
            } catch (BaseException e) {
                throw new ServiceFailure("1000",
                        "Method threw improper exception: " + e.getMessage());
            }

        } else {
            is = rd.getContentStream();
            try {
                return (Checksum) deserializeServiceType(Checksum.class, is);
            } catch (JiBXException e) {
                throw new ServiceFailure("500",
                        "Could not deserialize the returned Checksum: " + e.getMessage());
            }
        }
        return null;
    }

    /**
     * get the log records from a resource with the specified guid.
     */
    public Log getLogRecords(AuthToken token, Date fromDate, Date toDate,
            Event event) throws InvalidToken, ServiceFailure, NotAuthorized,
            InvalidRequest, NotImplemented {
        String resource = Constants.RESOURCE_LOG + "?";
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
        ResponseData rd = sendRequest(token, resource, Constants.GET, params, null, null, null);
        int code = rd.getCode();
        if (code != HttpURLConnection.HTTP_OK) {
            InputStream errorStream = rd.getErrorStream();
            try {
                deserializeAndThrowException(code, errorStream);
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
