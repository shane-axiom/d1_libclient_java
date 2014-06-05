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

import org.dataone.client.MNode;
import org.dataone.client.NodeLocator;
import org.dataone.client.impl.rest.CNode;
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

public class NodeListNodeLocator extends NodeLocator {

	protected Map<String, String> baseUrlMap;
	
	protected MultipartRestClient restClient;
	
	protected static final Integer DEFAULT_TIMEOUT_SECONDS = 30;
	
	
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
	 * not put into the map via the API, an MNode is not returned. 
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
				mn = new HttpMNode(this.restClient, this.baseUrlMap.get(nodeRef.getValue()) );
				super.putMNode(nodeRef, mn);
			}
		}
		return mn;
	}
	
	
	/**
	 * Gets the CNode associated with the NodeReference.  The CNode is lazy-
	 * instantiated at the first request.  If the NodeReference does not exist
	 * in the NodeList passed into the constructor, and an instantiated CNode is
	 * not put into the map via the API, a CNode is not returned. 
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
				super.putCNode(nodeRef, cn);			
			}
		}
		return cn;
	}	
}
