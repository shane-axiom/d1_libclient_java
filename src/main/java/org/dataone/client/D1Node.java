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

import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;

/**
 * The common non-service-API interface all MNodes or CNodes must implement. It
 * includes methods that associate a NodeId and baseUrl (found externally in the
 * NodeList) to the MNode or CNode,
 * allowing introspection and interoperability with NodeLocators.
 * 
 * It also allows MNodes and CNodes to be seen as the same type, although no
 * common service-API methods are not visible, and would require a cast to an
 * MNode or CNode first.
 * 
 * @author rnahf
 *
 */
public interface D1Node {

	/**
	 * Get the base service URL for the associated Member or Coordinating Node.
	 * The setter method is not included since that should be unchanging.  Rather,
	 * it is left to implementations to determine how to set it (Constructor or
	 * non interface method)
	 *
	 * @return
	 */
	public String getNodeBaseServiceUrl();

	/**
     * Get the NodeId associated with the D1Node
     * 
     * @return String representation of the NodeId 
     */
    public NodeReference getNodeId();

    /**
     * Set the NodeId associated with the D1Node
     * 
     * @param nodeId 
     */
    public void setNodeId(NodeReference nodeId);
    
    
    /**
     * Set the Nodetype for the D1Node
     * @param nodeType 
     */
    public void setNodeType(NodeType nodeType);
    
    /**
     * Get the NodeType associated with the D1Node
     * @return NodeType 
     */
    public NodeType getNodeType();
	
    
    /**
     * A method to provide information about the latestRequest from the D1Node
     * 
     * @return
     */
    public String getLatestRequestUrl();

}
