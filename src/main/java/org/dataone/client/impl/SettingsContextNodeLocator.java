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
import java.net.URISyntaxException;
import java.util.Map;

import org.dataone.client.CNode;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.impl.rest.DefaultHttpMultipartRestClient;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeList;

/**
 * This implementation of NodeLocator uses property defined in the configuration
 * to determine the NodeList to use for the mapping NodeReferences to baseUrls.
 * 
 * As with NodeListNodeLocator, these should be relied on instead of using the 
 * put methods.  The MNode and CNode objects are instantiated the first time 
 * they are requested from the get methods.
 * 
 * Use of the put methods will replace the current D1Node associated with the NodeReference,
 * so should be used with care care.
 * 
 * While most applications require / desire to have only one instance of a NodeLocator,
 * (that is Singleton behavior) this class does not do that, to support applications
 * that work across environments.     See NodeLocatorSingleton for this.
 * 
 
 * 
 * @author rnahf
 *
 */
public class SettingsContextNodeLocator extends NodeListNodeLocator {

	protected Map<String, String> baseUrlMap;
	
	protected MultipartRestClient restClient;
	
	protected static final Integer DEFAULT_TIMEOUT_SECONDS = 30;
	
	/**
	 * Creates a NodeLocator using default timeout settings (30 sec), and 
	 * using a DefaultHttpMultipartRestClient. It uses the property D1Client.CN_URL
	 * accessible through the Settings class.
	 * @throws ClientSideException 
	 */
	public SettingsContextNodeLocator() 
	throws NotImplemented, ServiceFailure, ClientSideException {
		this(DEFAULT_TIMEOUT_SECONDS);
	}
	
	public SettingsContextNodeLocator(Integer timeoutSeconds) 
	throws NotImplemented, ServiceFailure, ClientSideException {
		this(new DefaultHttpMultipartRestClient(timeoutSeconds));
	}

	
	public SettingsContextNodeLocator(MultipartRestClient mrc) 
	throws NotImplemented, ServiceFailure, ClientSideException 
	{
		super(determineFromSettings(mrc), mrc);
	}

	
	private static NodeList determineFromSettings(MultipartRestClient mrc) 
	throws ClientSideException, NotImplemented, ServiceFailure 
	{
		
		// get the CN URI
        String cnUri = Settings.getConfiguration().getString("D1Client.CN_URL");
        
        CNode cn;
		try {
			cn = buildCNode( mrc, new URI(cnUri) );
		} catch (URISyntaxException e) {
			throw new ClientSideException("Failed to build a CNode from provided class name.",e);
		}

        return cn.listNodes();
	}
}
