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

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.dataone.client.exception.ClientSideException;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;

/**
 * An abstract Service Locator class to resolve NodeReferences into MNode or 
 * CNode objects.  In the standard implementation, the NodeLocator would 
 * populate its nodes from a cn.NodeList call.
 *  
 * @author rnahf
 *
 */
public abstract class NodeLocator {

	/** this property can be re-initialized by concrete subclasses */
	// TODO: this is here to avoid needing to check for NPEs in methods, 
	// but should it be a tree map? or the simpler HashMap?  
	protected Map<NodeReference, D1Node> nodeMap = new TreeMap<NodeReference,D1Node>();
	
	/**
	 * Puts a constructed MNode into the NodeLocator.
	 * @param nodeRef
	 * @param mnode
	 */
	//TODO: is the NodeReference parameter necessary, if contained in the MNode itself?
	public void putMNode(NodeReference nodeRef, MNode mnode) {
		nodeMap.put(nodeRef, mnode);
	}
	
	/**
	 * Puts a constructed CNode into the NodeLocator.
	 * @param nodeRef
	 * @param cnode
	 */
	//TODO: is the NodeReference parameter necessary, if contained in the CNode itself?
	public void putCNode(NodeReference nodeRef, CNode cnode) {
		nodeMap.put(nodeRef, cnode);
	}
	
	/**
	 * Return an MNode associated with the nodeReference parameter, or 
	 * throw a ClientSideException
	 * 
	 * @param nodeReference
	 * @return
	 * @throws ClientSideException
	 */
	public MNode getMNode(NodeReference nodeReference) 
	throws ClientSideException 
	{
		D1Node d1n = nodeMap.get(nodeReference);
		if (d1n == null) 
			throw new ClientSideException("No node found for " + nodeReference.getValue(), null);
		
		if (d1n.getNodeType() != NodeType.MN) 
			throw new ClientSideException("The node is not of type Member Node.", null);
		
		return (MNode) nodeMap.get(nodeReference);
	}
	
	/**
	 * Return a CNode associated with the nodeReference parameter, or 
	 * throw a ClientSideException.
	 * 
	 * @param nodeReference
	 * @return
	 * @throws ClientSideException
	 */
	public CNode getCNode(NodeReference nodeReference) 
	throws ClientSideException 
	{
		D1Node d1n = nodeMap.get(nodeReference);
		if (d1n == null) 
			throw new ClientSideException("No node found for " + nodeReference.getValue(), null);
		
		if (d1n.getNodeType() != NodeType.CN) 
			throw new ClientSideException("The node is not of type Coordinating Node.",null);
		
		return (CNode) nodeMap.get(nodeReference);
	}
	
	
	/**
	 * Return an MNode associated with the baseUrl parameter, or 
	 * throw a ClientSideException
	 * 
	 * @param baseUrl
	 * @return
	 * @throws ClientSideException
	 */
	public MNode getMNode(String baseUrl) 
	throws ClientSideException
	{
		for (Entry<NodeReference, D1Node> en : nodeMap.entrySet()) {
			if (baseUrl.equals(en.getValue().getNodeBaseServiceUrl())) {
				return (MNode) en.getValue();
			}
		}
		throw new ClientSideException("No node found for " + baseUrl);
	}
	
	
	/**
	 * Return a CNode associated with the baseUrl parameter, or 
	 * throw a ClientSideException
	 * 
	 * @param baseUrl
	 * @return
	 * @throws ClientSideException
	 */
	public CNode getCNode(String baseUrl) 
	throws ClientSideException
	{
		for (Entry<NodeReference, D1Node> en : nodeMap.entrySet()) {
			if (baseUrl.equals(en.getValue().getNodeBaseServiceUrl())) {
				return (CNode) en.getValue();
			}
		}
		throw new ClientSideException("No node found for " + baseUrl);
	}
	
	/**
	 * Returns the set of NodeReferences in the NodeLocator
	 * 
	 * @return
	 */
	public Set<NodeReference> listD1Nodes() 
	{
		return nodeMap.keySet();
	}
	
	/**
	 * Returns the set of NodeReferences of D1Nodes matching the nodeType parameter 
	 * @param nodeType
	 * @return
	 */
	public Set<NodeReference> listD1Nodes(NodeType nodeType) 
	{
		Set<NodeReference> resultSet = new TreeSet<NodeReference>();
		for (Entry<NodeReference, D1Node> n : this.nodeMap.entrySet()) {
			if (n.getValue().getNodeType() == nodeType) {
				resultSet.add(n.getKey());
			}
		}
		return resultSet;
	}
	
	/**
	 * This method should return the favored CNode, so that a CN can be obtained
	 * without NodeReference or serviceBaseUrl.  If none can be determined, a
	 * ClientSideException should be thrown.
	 * 
	 * @return
	 */
	public abstract CNode getCNode() throws ClientSideException;
}
