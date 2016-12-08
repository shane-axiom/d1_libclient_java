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
package org.dataone.client.v1.impl;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.dataone.client.D1NodeFactory;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.rest.DefaultHttpMultipartRestClient;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.client.v1.CNode;
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
 * so should be used with care.
 * 
 * While most applications require / desire to have only one instance of a NodeLocator,
 * with Singleton or Monostate behavior, this class does not do that, to support applications
 * that work across environments.     See org.dataone.client.itk.D1Client for this.
 * 
 
 * 
 * @author rnahf
 *
 */
public class SettingsContextNodeLocator extends NodeListNodeLocator {

    protected Map<String, String> baseUrlMap;
//  protected MultipartRestClient restClient;
    protected CNode designatedCN;
    
//  protected static final Integer DEFAULT_TIMEOUT_SECONDS = 30;
    
    /**
     * Creates a NodeLocator using DefaultHttpMultipartRestClient. 
     * It uses the property D1Client.CN_URL
     * accessible through the Settings class.
     * @throws ClientSideException 
     * @throws IOException 
     */
    public SettingsContextNodeLocator() 
    throws NotImplemented, ServiceFailure, ClientSideException, IOException {
        this(new DefaultHttpMultipartRestClient());
    }

    
    public SettingsContextNodeLocator(MultipartRestClient mrc) 
            throws NotImplemented, ServiceFailure, ClientSideException {
        
        super(getNodeListFromSettingsCN(mrc), mrc);
        this.designatedCN = getCnFromSettings(mrc);
    }

    
    @Override
    /**
     * Returns the CNode built using the D1Client.CN_URL property.  This is different
     * behavior from the superclass NodeListNodeLocator, which uses the given NodeList
     * to choose by an algorithm that favors the Round Robin CN.
     * 
     * This implementation guarantees that if a specific CN is specified by the CN_URL,
     * it is always used.  Important for CN usage.
     */
    public CNode getCNode() {
        return designatedCN;
    }
    
    
    /**
     * Starting from Settings CN_URL, builds a CNode and calls listNodes.  This
     * allows for updates to the CN_URL, if picked up by Settings class, to be
     * used.
     *  
     * @param mrc
     * @return
     * @throws ClientSideException
     * @throws NotImplemented
     * @throws ServiceFailure
     */
    private static NodeList getNodeListFromSettingsCN(MultipartRestClient mrc) 
            throws ClientSideException, NotImplemented, ServiceFailure {
        
        return getCnFromSettings(mrc).listNodes();
    }


    /**
     * 
     * @param mrc
     * @return
     * @throws ClientSideException - truly client side errors whilst building the CNode
     */
    private static CNode getCnFromSettings(MultipartRestClient mrc) 
            throws ClientSideException { 

        // get the CN URI
        String cnUri = Settings.getConfiguration().getString("D1Client.CN_URL");

        // use the alternate implementation class from properties file in case it's set
        String cnClassName = Settings.getConfiguration().getString("D1Client.cnClassName");

        CNode cn;
        String uri = null;
        try {
            if (cnClassName == null) {
                uri = cnUri;
                cn = D1NodeFactory.buildNode(CNode.class, mrc, new URI(cnUri));
            } else {
                uri = cnClassName;
                cn = D1NodeFactory.buildNode(CNode.class, mrc, new URI(cnClassName));
                Method setBaseUrlMethod = cn.getClass().getMethod("setNodeBaseServiceUrl", new Class[]{String.class});
                setBaseUrlMethod.invoke(cn, cnUri);
            }

        } catch (URISyntaxException e) {
            throw new ClientSideException("Failed to build a CNode from provided CN baseUri: " + uri,e);
        } catch (NoSuchMethodException e) {
            throw new ClientSideException("Failed to find the setNodeBaseServiceUrl via reflection from the instantiated CN class: " + cnClassName,e);
        } catch (IllegalAccessException e) {
            throw new ClientSideException("Failed to set the nodeBaseServiceUrl via reflection from the instantiated CN class: " + cnClassName,e);
        } catch (InvocationTargetException e) {
            throw new ClientSideException("Failed to set the nodeBaseServiceUrl via reflection from the instantiated CN class: " + cnClassName,e);
        }
        if (cn == null) {
            // don't know how got here...
            throw new ClientSideException("Libclient error:  CNode return value would have been null!!!");
        }
        return cn;
    }

}
