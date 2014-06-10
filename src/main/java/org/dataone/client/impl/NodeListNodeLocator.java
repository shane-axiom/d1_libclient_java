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

import java.util.Map;

import org.dataone.client.CNode;
import org.dataone.client.MNode;
import org.dataone.client.NodeLocator;
import org.dataone.client.impl.rest.DefaultHttpMultipartRestClient;
import org.dataone.client.impl.rest.HttpCNode;
import org.dataone.client.impl.rest.HttpMNode;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeList;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.util.NodelistUtil;
/**
 * This implementation of NodeLocator uses a NodeList document to populate
 * the internal maps that have all of the registered MNodes and CNodes, instead
 * of relying on the put methods.  The MNode and CNode objects are 
 * instantiated the first time they are requested from the get methods.
 * 
 * Calling putMNode, for example, will replace the current MNode associated with
 * the NodeReference, so use with care.  
 * 
 * @author rnahf
 *
 */
public class NodeListNodeLocator extends NodeLocator {

	protected Map<String, String> baseUrlMap;
	
	protected MultipartRestClient restClient;
	
	protected static final Integer DEFAULT_TIMEOUT_SECONDS = 30;
	
	/**
	 * Creates a NodeLocator using default timeout settings (30 sec), and 
	 * using a DefaultHttpMultipartRestClient. It uses the property D1Client.CN_URL
	 * accessible through the Settings class.
	 */
	public NodeListNodeLocator() 
	throws NotImplemented, ServiceFailure {
		this(DEFAULT_TIMEOUT_SECONDS);
	}
	
	public NodeListNodeLocator(Integer timeoutSeconds) 
	throws NotImplemented, ServiceFailure {
		this(new DefaultHttpMultipartRestClient(timeoutSeconds));
	}

	
	public NodeListNodeLocator(MultipartRestClient mrc) 
	throws NotImplemented, ServiceFailure {
		this.restClient = mrc;

		// get the CN URL
        String cnUrl = Settings.getConfiguration().getString("D1Client.CN_URL");
        
        CNode cn = new HttpCNode( this.restClient, cnUrl );

        NodeList nl = cn.listNodes();
        this.baseUrlMap = NodelistUtil.mapNodeList(nl);
	}
	
	public NodeListNodeLocator(NodeList nl, MultipartRestClient mrc) {
		this.baseUrlMap = NodelistUtil.mapNodeList(nl);
		this.restClient = mrc;
	}
	
	
	/**
	 * Gets the MNode associated with the NodeReference.  The MNode is lazy-
	 * instantiated at the first request.  If the NodeReference does not exist
	 * in the NodeList passed into the constructor, and an instantiated MNode is
	 * not put into the registry, and an MNode is not returned. 
	 */
	@Override
	public MNode getMNode(NodeReference nodeRef) 
	{ 	
		MNode mn = super.getMNode(nodeRef);
		
		if (mn == null)
		{	
		    // not instantiated yet, so instantiate and put in the map
			if (this.baseUrlMap.containsKey(nodeRef.getValue())) 
			{
				mn = new HttpMNode(this.restClient, 
						this.baseUrlMap.get(nodeRef.getValue()) 
						);
				mn.setNodeId(nodeRef);
				putMNode(nodeRef, mn);
			}
		}
		return mn;
	}
	
	
	/**
	 * Gets the CNode associated with the NodeReference.  The CNode is lazy-
	 * instantiated at the first request.  If the NodeReference does not exist
	 * in the NodeList passed into the constructor, and an instantiated CNode is
	 * not put into the registry, and a CNode is not returned. 
	 */
	@Override
	public CNode getCNode(NodeReference nodeRef) 
	{	
		CNode cn = super.getCNode(nodeRef);
		
		if (cn == null) 
		{	
		    // not instantiated yet, so instantiate and put in the map
			if (this.baseUrlMap.containsKey(nodeRef.getValue())) 
			{
				cn = new HttpCNode( 
						this.restClient,
						this.baseUrlMap.get(nodeRef.getValue()) 
						);
				cn.setNodeId(nodeRef);
				putCNode(nodeRef, cn);			
			}
		}
		return cn;
	}
}
