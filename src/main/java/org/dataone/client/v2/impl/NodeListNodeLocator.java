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
package org.dataone.client.v2.impl;

import java.net.URI;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Set;

import org.dataone.client.D1NodeFactory;
import org.dataone.client.NodeLocator;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.client.v2.CNode;
import org.dataone.client.v2.MNode;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v2.util.NodelistUtil;
import org.dataone.service.types.v2.Node;
import org.dataone.service.types.v2.NodeList;

/**
 * This implementation of NodeLocator uses a NodeList document to populate
 * the internal maps that have all of the registered MNodes and CNodes, instead
 * of relying on the put methods.  The MNodes and CNodes created provide API methods
 * at the same version as the NodeListNodeLocator, regardless of whether the
 * referenced nodes implement those methods or not. Clients should call the methods
 * or use NodeListUtil to determine registered services for each node.
 * 
 * Use of the superclass's put methods will replace the current D1Node associated 
 * with the NodeReference, so should be used with care.
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
	protected MultipartRestClient client;
	
	/* the cnList is the list of CNodes that will be returned by getCN */
	protected Deque<CNode> cnList = new LinkedList<CNode>();  //LL implements Deque interface

	/**
	 * The constructor populates the NodeLocator from MN and CN nodes from the
	 * give Nodelist, building D1Node implementations using the MultipartRestClient.
	 * It also initializes the cnList that is used by getCNode.
	 * 
	 * @param nl - the nodeList to build the NodeLocator map with
	 * @param mrc - the MultipartRestClient to associate with the D1Nodes when building the map
	 * @throws ClientSideException
	 */
	public NodeListNodeLocator(NodeList nl, MultipartRestClient mrc) 
	throws ClientSideException 
	{
		this.nodeList = nl;
		this.client = mrc;

		// puts the nodes in the nodeList into the NodeLocator
		if (this.nodeList != null) {
			for (Node node: nl.getNodeList()) {
				if (node.getType().equals(NodeType.MN)) {
					super.putNode(
							node.getIdentifier(),
                            D1NodeFactory.buildNode(MNode.class, this.client, URI.create(node.getBaseURL()))
							);
				} else if (node.getType().equals(NodeType.CN)) {
					super.putNode(
							node.getIdentifier(),
                            D1NodeFactory.buildNode(CNode.class, this.client, URI.create(node.getBaseURL()))
							);
				}
			}	
			initCnList();
		}
	}
	
	
    @Override
    /**
     * Returns a CN if there is one in the NodeList.  Tries to determine the 
     * round-robin CN from the description field of the Node.  Otherwise, chooses
     * one of the set of CNs (rotating through each CNode with each successive call)
     * 
     * @throws ClientSideException - if no CNs were in the provided NodeList
     */
    public CNode getCNode() throws ClientSideException {
        
        if (cnList == null)
            throw new ClientSideException("LibClient Error: The CnList has not been initialized!!!");
        
        if (cnList.size() == 1)
            return cnList.getLast();
            
        if (cnList.size() == 0) 
            throw new ClientSideException("No CNs are registered in the NodeLocator");
        
        CNode nextCN = cnList.removeFirst();
        cnList.addLast(nextCN);
        return nextCN;
    }

    /**
     * Determines which CNodes will be part of the cnList.  If there is a Round
     * Robin CN listed in the NodeList, it will be used, otherwise, all of the 
     * other CNs listed will be used (and rotated through the getCN call).
     * 
     * @throws ClientSideException
     */
    public void initCnList() throws ClientSideException {
        
        // see if there's a round-robin cn to use
        if (this.nodeList != null) {
            Set<Node> cnSet = NodelistUtil.selectNodes(this.nodeList, NodeType.CN);
            // (selectNodes above guarantees a Set is returned, never null)
            Node rrCN = null;
            for (Node cn : cnSet) {
                if (cn.getDescription() != null) {
                    if (cn.getDescription().contains("Robin") ||
                            cn.getDescription().contains("robin")) {
                        rrCN = cn;
                        break;
                    }
                }
            }
            if (rrCN != null) {
                // found the round robin cn
                cnList = new LinkedList<CNode>();
                CNode rrCNode = D1NodeFactory.buildNode(CNode.class, client, URI.create(rrCN.getBaseURL()));
                rrCNode.setNodeId(rrCN.getIdentifier());
                cnList.add(rrCNode);
            } else {
                cnList = new LinkedList<CNode>();
                for (Node n : cnSet) {
                    CNode cNode = D1NodeFactory.buildNode(CNode.class, client, URI.create(n.getBaseURL()));
                    cNode.setNodeId(n.getIdentifier());
                    cnList.add(cNode);
                }
            }
        }
    }
}
