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

	/** this property needs to be initialized by concrete subclasses */
	protected Map<NodeReference, D1Node> nodeMap;
	

	public void putMNode(NodeReference nodeRef, MNode mnode) {
		nodeMap.put(nodeRef, mnode);
	}
	
	
	public void putCNode(NodeReference nodeRef, CNode cnode) {
		nodeMap.put(nodeRef, cnode);
	}
	
	
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
	
	
	public Set<NodeReference> listD1Nodes() 
	{
		return nodeMap.keySet();
	}
	
	 
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
