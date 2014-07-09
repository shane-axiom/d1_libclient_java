package org.dataone.client.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.dataone.client.CNode;
import org.dataone.client.MNode;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.impl.rest.DefaultHttpMultipartRestClient;
import org.dataone.client.impl.rest.HttpCNode;
import org.dataone.client.impl.rest.HttpMNode;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.client.types.D1TypeBuilder;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Subject;

public class D1NodeFactory {

	private MultipartRestClient restClient;
	
	public D1NodeFactory(MultipartRestClient mrc) {
		this.restClient = mrc;
	}
	
	public D1NodeFactory() {
		this.restClient = new DefaultHttpMultipartRestClient(30000);
	}
	
	/**
	 * Access the MultipartRestClient set by the parameterless constructor
	 * @return
	 */
	public MultipartRestClient getRestClient() {
		return this.restClient;
	}
	
	
	public CNode buildCNode(URI uri)
	throws ClientSideException
	{
		return D1NodeFactory.buildCNode(restClient, uri);
	}
	
	public MNode buildMNode(URI uri)
	throws ClientSideException
	{
		return D1NodeFactory.buildMNode(restClient, uri);
	}
	
	
	public static CNode buildCNode(MultipartRestClient mrc, URI uri) 
	throws ClientSideException 
	{
		// TOD check for nulls
		
		CNode builtCNode = null;
		
		if (uri.getScheme() == null) {
			throw new ClientSideException("CN uri had no scheme");
		}
		if(uri.getScheme().equals("java")) {
			builtCNode = buildJavaD1Node(CNode.class, uri);
		}
		else if (uri.getScheme().equals("http") || uri.getScheme().equals("https")) {
			// build the standard implementation
			builtCNode = new HttpCNode( mrc, uri.toString());
		}
		
		return builtCNode;
	} 

	
	public static MNode buildMNode(MultipartRestClient mrc, URI uri) 
	throws ClientSideException 
	{
		MNode builtMNode = null;
		if(uri.getScheme().equals("java")) {
			builtMNode = buildJavaD1Node(MNode.class, uri);
		}
		else if (uri.getScheme().equals("http") || uri.getScheme().equals("https")) {
			// build the standard implementation
			builtMNode = new HttpMNode( mrc, uri.toString());
		} else {
			throw new ClientSideException("No corresponding builder for URI scheme: " + uri.getScheme());
		}
		
		return builtMNode;
	} 
	
	/*
	 * 
	 */
	@SuppressWarnings("rawtypes")
	private static <T> T buildJavaD1Node(Class<T> domainClass, URI uri) throws ClientSideException 
	{
		try {
			String frag = uri.getFragment();
			String[] kvPairs = StringUtils.split(frag,"&");
			Class[] constructorParamTypes = new Class[kvPairs.length];
			Object[] initargs = new Object[kvPairs.length];
			for (int i=0;i<kvPairs.length; i++) {
				String[] pair = StringUtils.split(kvPairs[i], "=");
				if (pair[0].equals("Identifier")) {
					constructorParamTypes[i] = Identifier.class;
					initargs[i] = D1TypeBuilder.buildIdentifier(pair[1]);
				} else if (pair[0].equals("Subject")) {
					constructorParamTypes[i] = Subject.class;
					initargs[i] = D1TypeBuilder.buildSubject(pair[1]);
				} else if (pair[0].equals("NodeReference")) {
					constructorParamTypes[i] = NodeReference.class;
					initargs[i] = D1TypeBuilder.buildNodeReference(pair[1]);
				} else if (pair[0].equals("String")) {
					constructorParamTypes[i] = String.class;
					initargs[i] = pair[1];
				} else if (pair[0].equals("Integer")) {
					constructorParamTypes[i] = Integer.class;
					initargs[i] = new Integer(pair[1]);
				} else {
					throw new ClientSideException("Malformed fragment in nodeBaseUrl to form constructor arguments");
				}
			}
			Constructor c = Class.forName(uri.getSchemeSpecificPart()).getConstructor(constructorParamTypes);
			return (T) c.newInstance(initargs);
		} catch (SecurityException e) {
			throw new ClientSideException("Error in buildJavaMNodeFromURI",e);
		} catch (NoSuchMethodException e) {
			throw new ClientSideException("Error in buildJavaMNodeFromURI",e);
		} catch (ClassNotFoundException e) {
			throw new ClientSideException("Error in buildJavaMNodeFromURI",e);
		} catch (IllegalArgumentException e) {
			throw new ClientSideException("Error in buildJavaMNodeFromURI",e);
		} catch (InstantiationException e) {
			throw new ClientSideException("Error in buildJavaMNodeFromURI",e);
		} catch (IllegalAccessException e) {
			throw new ClientSideException("Error in buildJavaMNodeFromURI",e);
		} catch (InvocationTargetException e) {
			throw new ClientSideException("Error in buildJavaMNodeFromURI",e);
		} 
	}
	

}
