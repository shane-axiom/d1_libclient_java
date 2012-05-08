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
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;
import org.apache.http.client.ClientProtocolException;
import org.dataone.mimemultipart.SimpleMultipartEntity;
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
import org.dataone.service.exceptions.SynchronizationFailed;
import org.dataone.service.exceptions.UnsupportedType;
import org.dataone.service.mn.tier1.v1.MNCore;
import org.dataone.service.mn.tier1.v1.MNRead;
import org.dataone.service.mn.tier2.v1.MNAuthorization;
import org.dataone.service.mn.tier3.v1.MNStorage;
import org.dataone.service.mn.tier4.v1.MNReplication;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.DescribeResponse;
import org.dataone.service.types.v1.Event;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Log;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v1.Permission;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.Constants;
import org.dataone.service.util.D1Url;
import org.dataone.service.util.DateTimeMarshaller;
import org.jibx.runtime.JiBXException;

/**
 * MNode represents a MemberNode, and exposes the services associated with a
 * DataONE Member Node, allowing calling clients to call the services associated
 * with the node.
 * 
 * By necessity, behavior for null values differs slightly from the REST api. 
 * Except where noted, null values in method parameters will:
 * 1. throw a NotFound or InvalidRequest exception if the parameter is mapped to the URL path segment
 * 2. omit the url query parameter if the parameter is mapped to a URL query segment
 * 3. throw an InvalidRequest exception if the parameter is mapped to the message body
 * 
 * Session objects get passed to the underlying D1RestClient class, but  be passed to the httpClient request, as they are
 * pulled from the underlying filesystem of the local machine.
 * 
 * Java implementation of the following types:
 * Types.OctectStream - java.io.InputStream
 * unsignedlong - we use long to keep to a native type, although only half capacity of unsigned
 * Types.DateTime - java.util.Date
 * 
 *  @author Rob Nahf
 */

public class MNode extends D1Node 
implements MNCore, MNRead, MNAuthorization, MNStorage, MNReplication 
{
  
	protected static org.apache.commons.logging.Log log = LogFactory.getLog(MNode.class);
	
    /**
     * Construct a new client-side MNode (Member Node) object, 
     * passing in the base url of the member node for calling its services.
     * @param nodeBaseServiceUrl base url for constructing service endpoints.
     */
    public MNode(String nodeBaseServiceUrl) {
        super(nodeBaseServiceUrl); 
    }
   
    
    /**
     * Construct a new client-side MNode (Member Node) object, 
     * passing in the base url of the member node for calling its services,
     * and the Session to use for connections to that node. 
     * @param nodeBaseServiceUrl base url for constructing service endpoints.
     * @param session - the Session object passed to the CertificateManager
     *                  to be used for establishing connections
     */
    public MNode(String nodeBaseServiceUrl, Session session) {
        super(nodeBaseServiceUrl, session); 
    }
    
    
    public String getNodeBaseServiceUrl() {
    	D1Url url = new D1Url(super.getNodeBaseServiceUrl());
    	url.addNextPathElement(MNCore.SERVICE_VERSION);
    	log.debug("Node base service URL is: " + url.getUrl());
    	return url.getUrl();
    }
    

    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNCore.ping">see DataONE API Reference</a> } 
     */
    public Date ping() throws NotImplemented, ServiceFailure, InsufficientResources
    {
    	return super.ping();
    }


    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNCore.getLogRecords">see DataONE API Reference</a> }
     */
	public Log getLogRecords() 
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure 
	{
		return getLogRecords(null);
	}
	
	
    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNCore.getLogRecords">see DataONE API Reference</a> }
     */
	@Override
	public Log getLogRecords(Session session) 
	throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure 
	{
		Log theLog = null;
		try {
			theLog = super.getLogRecords(session);
		} catch (InsufficientResources e) {
			throw recastDataONEExceptionToServiceFailure(e);
		}
    	return theLog;
	}
 

	
    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNCore.getLogRecords">see DataONE API Reference</a> }
     * 
     */
    public Log getLogRecords(Date fromDate, Date toDate, Event event, String pidFilter, 
               Integer start, Integer count) 
    throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure
    {
        return getLogRecords(D1Node.sessionFromConstructor(), fromDate, toDate, event, pidFilter, start, count);
    }
    
    
    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNCore.getLogRecords">see DataONE API Reference</a> }
     * 
     */
    public Log getLogRecords(Session session, Date fromDate, Date toDate, 
               Event event, String pidFilter, Integer start, Integer count) 
    throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure
    {
    	Log theLog = null;
		try {
			theLog = super.getLogRecords(session, fromDate, toDate, event, pidFilter, start, count);
		} catch (InsufficientResources e) {
			throw recastDataONEExceptionToServiceFailure(e);
		}
    	return theLog;
    }

    
    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.listObjects">see DataONE API Reference</a> }
     */
	public ObjectList listObjects() 
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure 
	{
		return listObjects(null);
	}
    
    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.listObjects">see DataONE API Reference</a> }
     */
	@Override
	public ObjectList listObjects(Session session) 
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure 
	{
		return super.listObjects(session);
	}

	
    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.listObjects">see DataONE API Reference</a> }
     */	
	public ObjectList listObjects(Date fromDate, Date toDate, ObjectFormatIdentifier formatid,
			Boolean replicaStatus, Integer start, Integer count)
	throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure 
	{
		return super.listObjects(D1Node.sessionFromConstructor(),fromDate,toDate,formatid,replicaStatus,start,count);
	}
	
    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.listObjects">see DataONE API Reference</a> }
     */	
	@Override
	public ObjectList listObjects(Session session, Date fromDate,
			Date toDate, ObjectFormatIdentifier formatid,
			Boolean replicaStatus, Integer start, Integer count)
			throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented,
			ServiceFailure 
	{
		return super.listObjects(session,fromDate,toDate,formatid,replicaStatus,start,count);
	}

	
	/**
	 * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNCore.getCapabilities">see DataONE API Reference</a> }
	 */
    public Node getCapabilities() 
    throws NotImplemented, ServiceFailure
    {
    	// assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_NODE);

        // send the request
        D1RestClient client = new D1RestClient();
        Node node = null;

        try {
        	InputStream is = client.doGetRequest(url.getUrl());
        	node = deserializeServiceType(Node.class, is);
        } catch (BaseException be) {
            if (be instanceof NotImplemented)    throw (NotImplemented) be;
            if (be instanceof ServiceFailure)    throw (ServiceFailure) be;
                    
            throw recastDataONEExceptionToServiceFailure(be);
        } 
        catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
        catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

        finally {
        	setLatestRequestUrl(client.getLatestRequestUrl());
        	client.closeIdleConnections();
        }
        return node;
    }

    
    

    public InputStream get(Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound, InsufficientResources
    {
    	return super.get(D1Node.sessionFromConstructor(), pid);
    }  

    
    public InputStream get(Session session, Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound, InsufficientResources
    {
    	return super.get(session, pid);
    }


    public SystemMetadata getSystemMetadata(Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
    	return super.getSystemMetadata(D1Node.sessionFromConstructor(), pid);
    }   
    
    public SystemMetadata getSystemMetadata(Session session, Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
    	return super.getSystemMetadata(session,pid);
    }



    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.describe">see DataONE API Reference</a> } 
     */
    public DescribeResponse describe(Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
    	return super.describe(D1Node.sessionFromConstructor(),pid);
    }
    
    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.describe">see DataONE API Reference</a> } 
     */
    public DescribeResponse describe(Session session, Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {
    	return super.describe(session,pid);
    }


    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.getChecksum">see DataONE API Reference</a> } 
     */
    public Checksum getChecksum(Identifier pid, String checksumAlgorithm)
    throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    { 
    	return getChecksum(D1Node.sessionFromConstructor(), pid, checksumAlgorithm);
    }
    
    
    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.getChecksum">see DataONE API Reference</a> } 
     */
    public Checksum getChecksum(Session session, Identifier pid, String checksumAlgorithm)
    throws InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound
    {    	
        // assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_CHECKSUM);
        if (pid != null)
        	url.addNextPathElement(pid.getValue());
        
    	url.addNonEmptyParamPair("checksumAlgorithm", checksumAlgorithm);

        // send the request
        D1RestClient client = new D1RestClient(determineSession(session));
        Checksum checksum = null;

        try {
        	InputStream is = client.doGetRequest(url.getUrl());
        	checksum = deserializeServiceType(Checksum.class, is);
        } catch (BaseException be) {
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof NotFound)               throw (NotFound) be;
                    
            throw recastDataONEExceptionToServiceFailure(be);
        } 
        catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
        catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

        finally {
        	setLatestRequestUrl(client.getLatestRequestUrl());
        	client.closeIdleConnections();
        }
        return checksum;
    }

    
    
    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.synchronizationFailed">see DataONE API Reference</a> } 
     */
    public boolean synchronizationFailed(SynchronizationFailed message)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure
    { 
        return synchronizationFailed(D1Node.sessionFromConstructor(), message);
    }
    
    
    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.synchronizationFailed">see DataONE API Reference</a> } 
     */
    public boolean synchronizationFailed(Session session, SynchronizationFailed message)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure
    {   	
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_ERROR);
    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
			mpe.addFilePart("message", message.serialize(SynchronizationFailed.FMT_XML));
		} catch (IOException e1) {
			throw recastClientSideExceptionToServiceFailure(e1);
		}
    	
        // send the request
        D1RestClient client = new D1RestClient(determineSession(session));

        InputStream is = null;
        try {
        	is = client.doPostRequest(url.getUrl(),mpe);
        	if (is != null)
				is.close();
        } catch (BaseException be) {
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
                    
            throw recastDataONEExceptionToServiceFailure(be);
        } 
        catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
        catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 
        
        finally {
        	setLatestRequestUrl(client.getLatestRequestUrl());
        	client.closeIdleConnections();
        }
        return true;
    }


    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNAuthorization.isAuthorized">see DataONE API Reference</a> } 
     */
    public boolean isAuthorized(Identifier pid, Permission action)
    throws ServiceFailure, InvalidRequest, InvalidToken, NotFound, NotAuthorized, NotImplemented
    {
    	return super.isAuthorized(D1Node.sessionFromConstructor(), pid, action);
    }
    
    
    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNAuthorization.isAuthorized">see DataONE API Reference</a> } 
     */
    public boolean isAuthorized(Session session, Identifier pid, Permission action)
    throws ServiceFailure, InvalidRequest, InvalidToken, NotFound, NotAuthorized, NotImplemented
    {
    	return super.isAuthorized(session, pid, action);
    }


	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.generateIdentifier">see DataONE API Reference</a> } 
	 */
	public  Identifier generateIdentifier(String scheme, String fragment)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest
	{
		return super.generateIdentifier(D1Node.sessionFromConstructor(), scheme, fragment);
	}
	
	/**
	 *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.generateIdentifier">see DataONE API Reference</a> } 
	 */
	public  Identifier generateIdentifier(Session session, String scheme, String fragment)
	throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest
	{
		return super.generateIdentifier(session, scheme, fragment);
	}


	
    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.create">see DataONE API Reference</a> } 
     */
    public  Identifier create(Identifier pid, InputStream object, 
            SystemMetadata sysmeta) 
    throws IdentifierNotUnique, InsufficientResources, InvalidRequest, InvalidSystemMetadata, 
        	InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, UnsupportedType
    {
        return create(D1Node.sessionFromConstructor(), pid, object,  sysmeta);
    }
     
		
	/**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.create">see DataONE API Reference</a> } 
     */
    public  Identifier create(Session session, Identifier pid, InputStream object, 
            SystemMetadata sysmeta) 
    throws IdentifierNotUnique, InsufficientResources, InvalidRequest, InvalidSystemMetadata, 
        	InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, UnsupportedType
    {
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);

    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
    	try {
    		mpe.addParamPart("pid", pid.getValue());
			mpe.addFilePart("object",object);
			mpe.addFilePart("sysmeta", sysmeta);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (JiBXException e) {
			throw recastClientSideExceptionToServiceFailure(e);	
		}

    	
    	D1RestClient client = new D1RestClient(determineSession(session));
    	Identifier identifier = null;

    	try {
    		InputStream is = client.doPostRequest(url.getUrl(),mpe);
    		 identifier = deserializeServiceType(Identifier.class, is);
        } catch (BaseException be) {
            if (be instanceof IdentifierNotUnique)    throw (IdentifierNotUnique) be;
            if (be instanceof InsufficientResources)  throw (InsufficientResources) be;
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
            if (be instanceof InvalidSystemMetadata)  throw (InvalidSystemMetadata) be;
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof UnsupportedType)        throw (UnsupportedType) be;
                    
            throw recastDataONEExceptionToServiceFailure(be);
        } 
        catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
        catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

        finally {
        	setLatestRequestUrl(client.getLatestRequestUrl());
        	client.closeIdleConnections();
        }
        return identifier;
    }


    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.update">see DataONE API Reference</a> } 
     */
    public  Identifier update(Identifier pid, InputStream object, 
            Identifier newPid, SystemMetadata sysmeta) 
        throws IdentifierNotUnique, InsufficientResources, InvalidRequest, InvalidSystemMetadata, 
            InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, UnsupportedType,
            NotFound
    {
        return update(D1Node.sessionFromConstructor(), pid, object, newPid, sysmeta);
    }
    
    
    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.update">see DataONE API Reference</a> } 
     */
    public  Identifier update(Session session, Identifier pid, InputStream object, 
            Identifier newPid, SystemMetadata sysmeta) 
        throws IdentifierNotUnique, InsufficientResources, InvalidRequest, InvalidSystemMetadata, 
            InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, UnsupportedType,
            NotFound
    {
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_OBJECTS);
    	url.addNextPathElement(pid.getValue());

    	SimpleMultipartEntity mpe = new SimpleMultipartEntity();
//    	mpe.addParamPart("newPid", EncodingUtilities.encodeUrlQuerySegment(newPid.getValue()));
    	mpe.addParamPart("newPid", newPid.getValue());
    	try {
			mpe.addFilePart("object",object);
			mpe.addFilePart("sysmeta", sysmeta);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (JiBXException e) {
			throw recastClientSideExceptionToServiceFailure(e);	
		}
    	  	
    	D1RestClient client = new D1RestClient(determineSession(session));
    	Identifier identifier = null;
    	
    	try {
    		InputStream is = client.doPutRequest(url.getUrl(),mpe);
    		identifier = deserializeServiceType(Identifier.class, is);
        } catch (BaseException be) {
            if (be instanceof IdentifierNotUnique)    throw (IdentifierNotUnique) be;
            if (be instanceof InsufficientResources)  throw (InsufficientResources) be;
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
            if (be instanceof InvalidSystemMetadata)  throw (InvalidSystemMetadata) be;
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof UnsupportedType)        throw (UnsupportedType) be;
            if (be instanceof NotFound)               throw (NotFound) be;
                    
            throw recastDataONEExceptionToServiceFailure(be);
        } 
        catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
        catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 

        finally {
        	setLatestRequestUrl(client.getLatestRequestUrl());
        	client.closeIdleConnections();
        }
        return identifier;
    }


    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.archive">see DataONE API Reference</a> }
     */
    public  Identifier archive( Identifier pid)
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
    {
        return  super.archive(D1Node.sessionFromConstructor(),  pid);
    }
   
    
    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.archive">see DataONE API Reference</a> }
     */
    public  Identifier archive(Session session, Identifier pid)
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
    {
        return super.archive(session, pid);
    }
    
    
    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.delete">see DataONE API Reference</a> }
     */
    public  Identifier delete( Identifier pid)
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
    {
        return  delete(D1Node.sessionFromConstructor(),  pid);
    }
   
    
    /**
     * {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNStorage.delete">see DataONE API Reference</a> }
     */
    public  Identifier delete(Session session, Identifier pid)
        throws InvalidToken, ServiceFailure, NotAuthorized, NotFound, NotImplemented
    {
    	 return super.delete(D1Node.sessionFromConstructor(), pid);
    }

    
    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNAuthorization.systemMetadataChanged">see DataONE API Reference</a> } 
     */
    public boolean systemMetadataChanged(Identifier pid, long serialVersion,
        	Date dateSystemMetadataLastModified)
        throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest
    {
        return systemMetadataChanged(D1Node.sessionFromConstructor(), pid, serialVersion, dateSystemMetadataLastModified);
    }
    
    
    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNAuthorization.systemMetadataChanged">see DataONE API Reference</a> } 
     */
    public boolean systemMetadataChanged(Session session, Identifier pid, long serialVersion,
        	Date dateSystemMetadataLastModified)
        throws InvalidToken, ServiceFailure, NotAuthorized, NotImplemented, InvalidRequest
    {
    	D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_META_CHANGED);

		SimpleMultipartEntity mpe = new SimpleMultipartEntity();
		if (pid != null)
			mpe.addParamPart("pid", pid.getValue());
        mpe.addParamPart("dateSysMetaLastModified", 
                DateTimeMarshaller.serializeDateToUTC(dateSystemMetadataLastModified));
        mpe.addParamPart("serialVersion", String.valueOf(serialVersion));
		    	
		D1RestClient client = new D1RestClient(determineSession(session));
		InputStream is = null;
		try {
			is = client.doPostRequest(url.getUrl(), mpe);
			if (is != null)
				is.close();
        } catch (BaseException be) {
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
                    
            throw recastDataONEExceptionToServiceFailure(be);
        } 
        catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
        catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); }  
		
		finally {	
			setLatestRequestUrl(client.getLatestRequestUrl());
			client.closeIdleConnections();
		}
		return true;
    }
   
 
    
    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNReplication.replicate">see DataONE API Reference</a> } 
     */
    public boolean replicate(SystemMetadata sysmeta, NodeReference sourceNode) 
    throws NotImplemented, ServiceFailure, NotAuthorized, InvalidRequest, InvalidToken,
        InsufficientResources, UnsupportedType
    {
        return replicate(D1Node.sessionFromConstructor(), sysmeta, sourceNode);
    }
    
    
    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNReplication.replicate">see DataONE API Reference</a> } 
     */
    public boolean replicate(Session session, SystemMetadata sysmeta, NodeReference sourceNode) 
    throws NotImplemented, ServiceFailure, NotAuthorized, InvalidRequest, InvalidToken,
        InsufficientResources, UnsupportedType
    {
		// assemble the url
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(), Constants.RESOURCE_REPLICATE);
    	
    	// assemble the context body
    	SimpleMultipartEntity smpe = new SimpleMultipartEntity();
    	if (sourceNode != null)
    		smpe.addParamPart("sourceNode", sourceNode.getValue());

    	try {
			smpe.addFilePart("sysmeta", sysmeta);
		} catch (IOException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		} catch (JiBXException e) {
			throw recastClientSideExceptionToServiceFailure(e);
		}
    	
    	D1RestClient client = new D1RestClient(determineSession(session));
    	InputStream is = null;
    	try {
			is = client.doPostRequest(url.getUrl(),smpe);
			if (is != null)
				is.close();
        } catch (BaseException be) {
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof InvalidRequest)         throw (InvalidRequest) be;
			if (be instanceof InvalidToken)	          throw (InvalidToken) be;
            if (be instanceof InsufficientResources)  throw (InsufficientResources) be;
            if (be instanceof UnsupportedType)        throw (UnsupportedType) be;
                    
            throw recastDataONEExceptionToServiceFailure(be);
        } 
        catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
        catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 
    	
    	finally {
    		setLatestRequestUrl(client.getLatestRequestUrl());
    		client.closeIdleConnections();
    	}
        return true;
    }


    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.getReplica">see DataONE API Reference</a> } 
     */
    public InputStream getReplica(Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound,
    InsufficientResources
    {
       return getReplica(D1Node.sessionFromConstructor(), pid);
    }
    
    
    /**
     *  {@link <a href=" http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html#MNRead.getReplica">see DataONE API Reference</a> } 
     */
    public InputStream getReplica(Session session, Identifier pid)
    throws InvalidToken, NotAuthorized, NotImplemented, ServiceFailure, NotFound,
    InsufficientResources
    {
        D1Url url = new D1Url(this.getNodeBaseServiceUrl(),Constants.RESOURCE_REPLICAS);
        if (pid != null)
        	url.addNextPathElement(pid.getValue());

        // send the request
        D1RestClient client = new D1RestClient(determineSession(session));
        ByteArrayInputStream bais = null;
        try {
        	byte[] bytes = IOUtils.toByteArray(client.doGetRequest(url.getUrl()));
        	bais = new ByteArrayInputStream(bytes);     	
        } catch (BaseException be) {
            if (be instanceof InvalidToken)           throw (InvalidToken) be;
            if (be instanceof NotAuthorized)          throw (NotAuthorized) be;
            if (be instanceof NotImplemented)         throw (NotImplemented) be;
            if (be instanceof ServiceFailure)         throw (ServiceFailure) be;
            if (be instanceof NotFound)               throw (NotFound) be;
            if (be instanceof InsufficientResources)  throw (InsufficientResources) be;
                    
            throw recastDataONEExceptionToServiceFailure(be);
        } 
        catch (ClientProtocolException e)  {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IllegalStateException e)    {throw recastClientSideExceptionToServiceFailure(e); }
        catch (IOException e)              {throw recastClientSideExceptionToServiceFailure(e); }
        catch (HttpException e)            {throw recastClientSideExceptionToServiceFailure(e); } 
        
        finally {
        	setLatestRequestUrl(client.getLatestRequestUrl());
        	client.closeIdleConnections();
        }
        return bais;
    }
}
