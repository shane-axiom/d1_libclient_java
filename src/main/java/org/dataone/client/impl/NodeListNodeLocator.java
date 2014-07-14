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

import java.net.URI;
import java.util.Set;

import org.dataone.client.CNode;
import org.dataone.client.NodeLocator;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeList;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.util.NodelistUtil;

/**
 * This implementation of NodeLocator uses a NodeList document to populate
 * the internal maps that have all of the registered MNodes and CNodes, instead
 * of relying on the put methods.  The MNode and CNode objects are 
 * instantiated the first time they are requested from the get methods.
 * 
 * Use of the put methods will replace the current D1Node associated with the NodeReference,
 * so should be used with care.
 * 
 * While most applications require / desire to have only one instance of a NodeLocator,
 * with Singleton or Monostate behavior, this class does not do that, to support applications
 * that work across environments.     See org.dataone.client.itk.D1Client for this.
 * 
 * @author rnahf
 *
 */
public class NodeListNodeLocator extends NodeLocator {

	protected NodeList nodeList;	
	protected MultipartRestClient restClient;

	
	public NodeListNodeLocator(NodeList nl, MultipartRestClient mrc) 
	throws ClientSideException 
	{
		this.nodeList = nl;
		this.restClient = mrc;

		if (this.nodeList != null) {
			for (Node node: nl.getNodeList()) {
				if (node.getType().equals(NodeType.MN)) {
					super.putMNode(
							node.getIdentifier(),
							D1NodeFactory.buildMNode(this.restClient, URI.create(node.getBaseURL()))
							);
				} else if (node.getType().equals(NodeType.CN)) {
					super.putCNode(
							node.getIdentifier(),
							D1NodeFactory.buildCNode(this.restClient, URI.create(node.getBaseURL()))
							);
				}
			}	
		}
	}
	

	
	@Override
	public CNode getCNode() throws ClientSideException
	{
		Set<Node> cns = NodelistUtil.selectNodes(nodeList, NodeType.CN);

		if (cns.isEmpty()) { 
			throw new ClientSideException("No CNs are registered in the NodeLocator");
		}
		for (Node node : cns) {
			if (node.getDescription() != null && node.getDescription().contains("Robin")) {
				return D1NodeFactory.buildCNode(restClient, URI.create(node.getBaseURL()));
			}
		}
		// get the first one
		Node n = cns.iterator().next();		
		return D1NodeFactory.buildCNode(restClient, URI.create(n.getBaseURL()));

	}
}
	

