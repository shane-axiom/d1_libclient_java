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

import org.dataone.service.types.v1.NodeReference;

/**
 * An abstract Service Locator class to resolve NodeReferences into MNode or 
 * CNode objects.  In the standard implementation, the NodeLocator populate 
 * would populate its nodes from a cn.NodeList call.
 *  
 * @author rnahf
 *
 */
public abstract class NodeLocator {

	private Map<String, MNode> MNodeMap;
	
	private Map<String, CNode> CNodeMap;
	
	
	public void putMNode(NodeReference nodeRef, MNode mnode) {
		MNodeMap.put(nodeRef.getValue(), mnode);
	}
	
	public void putCNode(NodeReference nodeRef, CNode cnode) {
		CNodeMap.put(nodeRef.getValue(), cnode);
	}
	  	
	public MNode getMNode(NodeReference nodeReference) {
		return MNodeMap.get(nodeReference);
	}
	
	public CNode getCNode(NodeReference nodeReference) {
		return CNodeMap.get(nodeReference);
	}
	
	
}
