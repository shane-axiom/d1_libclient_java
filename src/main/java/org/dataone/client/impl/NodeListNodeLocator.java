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
package org.dataone.client.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.dataone.client.CNode;
import org.dataone.client.D1Node;
import org.dataone.client.MNode;
import org.dataone.client.NodeLocator;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.impl.rest.HttpCNode;
import org.dataone.client.impl.rest.HttpMNode;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.client.types.D1TypeBuilder;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeList;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.util.NodelistUtil;

/**
 * This implementation of NodeLocator uses a NodeList document to populate
 * the internal maps that have all of the registered MNodes and CNodes, instead
 * of relying on the put methods.  The MNode and CNode objects are 
 * instantiated the first time they are requested from the get methods.
 * 
 * While most applications require / desire to have only one instance of a NodeLocator,
 * (that is Singleton behavior) this class does not do that, to support applications
 * that work across environments.     See NodeLocatorSingleton for this 
 * 
 * Calling putMNode, for example, will replace the current MNode associated with
 * the NodeReference, so use with care.  
 * 
 * @author rnahf
 *
 */
public class NodeListNodeLocator extends NodeLocator {

//	protected Map<String, String> baseUrlMap;
	protected NodeList nodeList;	
	protected MultipartRestClient restClient;

	
	public NodeListNodeLocator(NodeList nl, MultipartRestClient mrc) 
	throws ClientSideException 
	{
		this.nodeMap = new TreeMap<NodeReference, D1Node>();
		
		this.nodeList = nl;
		this.restClient = mrc;
//		this.baseUrlMap = NodelistUtil.mapNodeList(nl);

		for (Node node: nl.getNodeList()) {
			if (node.getType().equals(NodeType.MN)) {
				super.putMNode(
						node.getIdentifier(),
						buildMNode(this.restClient, URI.create(node.getBaseURL()))
						);
			} else if (node.getType().equals(NodeType.CN)) {
				super.putCNode(
						node.getIdentifier(),
						buildCNode(this.restClient, URI.create(node.getBaseURL()))
						);
			}
		}		
	}
	
	
//	/**
//	 * Gets the MNode associated with the NodeReference.  The MNode is lazy-
//	 * instantiated at the first request.  If the NodeReference does not exist
//	 * in the NodeList passed into the constructor, and an instantiated MNode is
//	 * not put into the registry, and an MNode is not returned. 
//	 * 
//	 * @throws ClientSideException if the NodeReference is not for an MNode
//	 */
//	@Override
//	public MNode getMNode(NodeReference nodeRef) throws ClientSideException 
//	{ 	
//		MNode mn = null;
//		try {
//			mn = super.getMNode(nodeRef);
//		}
//		catch (ClientSideException e) {
//			
//		    // not instantiated yet, so instantiate and put in the map
//			if (this.baseUrlMap.containsKey(nodeRef.getValue())) 
//			{
//				mn = new HttpMNode(this.restClient, 
//						this.baseUrlMap.get(nodeRef.getValue()) 
//						);
//				mn.setNodeId(nodeRef);
//				putMNode(nodeRef, mn);
//			}
//		}
//		return mn;
//	}
//	
//	
//	/**
//	 * Gets the CNode associated with the NodeReference.  The CNode is lazy-
//	 * instantiated at the first request.  If the NodeReference does not exist
//	 * in the NodeList passed into the constructor, and an instantiated CNode is
//	 * not put into the registry, and a CNode is not returned.
//	 *  
//	 * @throws ClientSideException if the NodeReference is not for a CNode
//	 */
//	@Override
//	public CNode getCNode(NodeReference nodeRef) throws ClientSideException 
//	{	
//		CNode cn = null;
//		try {
//			cn = super.getCNode(nodeRef);
//		}
//		catch (ClientSideException e) {
//
//			// not instantiated yet, so instantiate and put in the map
//			if (this.baseUrlMap.containsKey(nodeRef.getValue())) 
//			{
//				cn = new HttpCNode( 
//						this.restClient,
//						this.baseUrlMap.get(nodeRef.getValue()) 
//						);
//				cn.setNodeId(nodeRef);
//				putCNode(nodeRef, cn);			
//			}
//		}
//		return cn;
//	}
	
	@Override
	public CNode getCNode() throws ClientSideException
	{
		Set<Node> cns = NodelistUtil.selectNodes(nodeList, NodeType.CN);

		if (cns.isEmpty()) { 
			throw new ClientSideException("No CNs are registered in the NodeLocator");
		}
		for (Node node : cns) {
			if (node.getDescription() != null && node.getDescription().contains("Robin")) {
				return buildCNode(restClient, URI.create(node.getBaseURL()));
			}
		}
		// get the first one
		Node n = cns.iterator().next();		
		return buildCNode(restClient, URI.create(n.getBaseURL()));

	}


	
	
	protected static CNode buildCNode(MultipartRestClient mrc, URI uri) 
	throws ClientSideException 
	{
		CNode builtCNode = null;
		if(uri.getScheme().equals("java")) {
			// instantiate an instance of the class
			try {
				
				builtCNode = (CNode) Class.forName(uri.getSchemeSpecificPart()).newInstance();
			} catch (Exception e) {
				e.printStackTrace();
				throw new ClientSideException("",e);
			}
			// TODO: figure out if this needs to be populated
//				cn.setNodeBaseServiceUrl(cnUrl);
		}
		else if (uri.getScheme().equals("http") || uri.getScheme().equals("https")) {
			// build the standard implementation
			builtCNode = new HttpCNode( mrc, uri.toString());
		}
		
		return builtCNode;
	} 

	
	protected static MNode buildMNode(MultipartRestClient mrc, URI uri) 
	throws ClientSideException 
	{
		MNode builtMNode = null;
		if(uri.getScheme().equals("java")) {
			builtMNode = buildJavaMNode(uri);
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
	private static MNode buildJavaMNode(URI uri) throws ClientSideException 
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
			return (MNode) c.newInstance(initargs);
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
	

