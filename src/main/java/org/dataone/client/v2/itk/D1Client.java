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
 * 
 * $Id$
 */

package org.dataone.client.v2.itk;

import java.io.IOException;
import java.net.URI;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;

import org.apache.log4j.Logger;
import org.dataone.client.D1NodeFactory;
import org.dataone.client.NodeLocator;
import org.dataone.client.auth.X509Session;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.rest.DefaultHttpMultipartRestClient;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.client.utils.ExceptionUtils;
import org.dataone.client.v2.CNode;
import org.dataone.client.v2.MNode;
import org.dataone.client.v2.impl.NodeListNodeLocator;
import org.dataone.client.v2.impl.SettingsContextNodeLocator;
import org.dataone.service.cn.v2.CNCore;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Session;


/**
 * The D1Client class represents a client-side implementation of the DataONE
 * Service API. The class exposes the DataONE APIs as client methods, dispatches
 * the calls to the correct DataONE node, and then returns the results or throws
 * the appropriate exceptions.
 */
public class D1Client {

    private static NodeLocator nodeLocator;
    private static long lastNLRefresh = 0;
    private static long lastNLRefreshAttempt = 0;
    private static long NODELOCATOR_REFRESH_ATTEMPT_INTERVAL = 5000;
    private static final long NODELOCATOR_STALE_INTERVAL = 5 * 60000; // min * millisec/min 
    protected static MultipartRestClient multipartRestClient;
    
    final static Logger logger = Logger.getLogger(D1Client.class);
    
    /*
     * A class for thread safety, to allow static initialization of the
     * MultipartRestClient instance. If multipartRestClient didn't throw
     * exceptions, we could have simplified construct to a static initializer.
     * (We're getting lazy-loading for free here, too.)
     */
//    private static class MultipartRestClientHelper {
//        public static MultipartRestClient instance;
//        
//        // burying the static initialization in the class se we have a fancy
//        // way to handle exceptions. (convert to RuntimeException, then catch
//        // the Runtime exception in the getter.
//        static {
//            try {
//                instance = new DefaultHttpMultipartRestClient();
//                
//                // one time check for certificate issue
//                X509Session s = instance.getSession();
//                if (s != null) {
//                    try {
//                        s.checkValidity();
//                    } catch (CertificateExpiredException
//                            | CertificateNotYetValidException e) {
//                        throw new ClientSideException("Certificate Problem", e);
//                    }
//                }
//            } catch (IOException | ClientSideException e) {
//                RuntimeException re = new RuntimeException();
//                re.initCause(e);
//            }
//        }
//    }

    /*
     * Part of the thread-safe way to initialize the static MRC 
     */
//    protected static MultipartRestClient getMultipartRestClient()
//    throws IOException, ClientSideException {
//        try {
//            return MultipartRestClientHelper.instance;
//        } catch (RuntimeException re) {
//           if (re.getCause() instanceof IOException)
//               throw (IOException)re.getCause();
//           if (re.getCause() instanceof ClientSideException) 
//               throw (ClientSideException)re.getCause();
//           ClientSideException cse = new ClientSideException("UnexpectedException thrown!");
//           cse.initCause(re.getCause());
//           throw cse;
//        }
//    }
    protected static synchronized MultipartRestClient getMultipartRestClient() throws IOException, ClientSideException {
        if (multipartRestClient == null) {
            multipartRestClient = new DefaultHttpMultipartRestClient();
            // one time check for certificate issue
            X509Session s = multipartRestClient.getSession();
            if (s != null) {
                try {
                    s.checkValidity();
                } catch (CertificateExpiredException
                        | CertificateNotYetValidException e) {
                    multipartRestClient = null;
                    throw new ClientSideException("Certificate Problem", e);
                }
            }
        }
        return multipartRestClient;
    }
    
    
    


    /**
     * For testing, we can override the nodeLocator
     * @param nodeLocator
     */
    public static void setNodeLocator(NodeLocator nodeLocator) {
        D1Client.nodeLocator = nodeLocator;
    }
    
    
    /**
     * Get the cached CNode object for calling Coordinating Node services.
     * By default returns the production context CN, defined via the property
     * "D1Client.CN_URL". Use of D1Client in other contexts (non-production)
     * requires overriding or changing this property name, or calling the setCN
     * method.
     * See org.dataone.configuration.Settings class for details.
     *
     * Connects using the default session / certificate
     * @return the cn
     * @throws ServiceFailure
     * @throws NotImplemented
     */
    public static CNode getCN()
    throws ServiceFailure, NotImplemented
    {
        try { 
            bestAttemptRefreshNodeLocator();
            if (nodeLocator != null) 
                return (CNode) nodeLocator.getCNode();
            else
                throw new ServiceFailure("000","Could not get CNode from the underlying context (D1Client.CN_URL)");
            
        } catch (ClientSideException e1) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e1);
        }
        
    }


    /**
     * Get the cached CNode object for calling Coordinating Node services.
     * By default returns the production context CN, defined via the property
     * "D1Client.CN_URL".  Use of D1Client in other contexts (non-production)
     * requires overriding or changing this property name, or calling the setCN
     * method
     * See org.dataone.configuration.Settings class for details.
     *
     * @deprecated broken functionality, and unused.  use getCN() or getCN(baseUrl) instead
     *
     * @param session - the client session to be used in connections, null uses default behavior.
     * @return the cn
     * @throws ServiceFailure
     * @throws NotImplemented
     */
    @Deprecated
    public static CNode getCN(Session session)
    throws ServiceFailure, NotImplemented {
        
        // redirect to parameterless getCN method
        return getCN();
    }


    /**
     * Use this method to set the environment via the baseUrl to the environment's
     * Coordinating Node.  Doing so affects future calls using the NodeReferences -
     * only nodes registered in the context of the current CN will be findable
     * by NodeReference.
     * 
     * @param cnUrl
     * @throws NotImplemented
     * @throws ServiceFailure
     */
    public static void setCN(String cnUrl)
    throws NotImplemented, ServiceFailure
    {
        if (cnUrl == null)  cnUrl = "";
        try {
            CNCore cn = D1NodeFactory.buildNode(CNCore.class, getMultipartRestClient(), URI.create(cnUrl));
            // TODO: using a NodeListNodeLocator bypasses the designatedCN behavior of SettingsContextNodeLocator
            // should we be overriding the SettingsContext setting instead?
    		nodeLocator = new NodeListNodeLocator(cn.listNodes(), getMultipartRestClient());
    		lastNLRefresh = System.currentTimeMillis(); 
    	} catch (ClientSideException | IOException e) {
			ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
		}
    }

    /**
     * Returns a Member Node using the base service URL for the node.
     * @param mnBaseUrl the service URL for the Member Node
     * @return the mn at a particular URL
     * @throws ServiceFailure
     */
    public static MNode getMN(String mnBaseUrl) throws ServiceFailure
    {
    	bestAttemptRefreshNodeLocator();
    	
    	MNode mn = null;
    	if (nodeLocator != null) {
    		try {		
    			mn = (MNode) nodeLocator.getNode(mnBaseUrl);	
    		} 
    		catch (ClientSideException e) {
    			;  // that's ok, will try a different way
    		}
    	}
    	if (mn == null) {
    		try {
                mn = D1NodeFactory.buildNode(MNode.class, getMultipartRestClient(), URI.create(mnBaseUrl));
    		}
			catch (ClientSideException | IOException cse) {
				throw ExceptionUtils.recastClientSideExceptionToServiceFailure(cse);
			}
    	}
    	return mn;
    }


    /**
     * Returns a Coordinating Node using the base service URL to look up the node
     * in the existing environment.
     *
     * @param cnBaseUrl
     * @return
     * @throws ServiceFailure
     */
    //TODO: do we need this method?  When do we need to micro-manage which CN to connect to?
    public static CNode getCN(String cnBaseUrl) throws ServiceFailure
    {
        bestAttemptRefreshNodeLocator();
        
        CNode cn = null;	
    	if (nodeLocator != null) {
    		try {		
    			cn = (CNode) nodeLocator.getNode(cnBaseUrl);	
    		} 
    		catch (ClientSideException e) {
    			;  // that's ok, will try a different way
    		}
    	}
    	if (cn == null) {
    		try {
                return D1NodeFactory.buildNode(CNode.class, getMultipartRestClient(), URI.create(cnBaseUrl));
    		}
			catch (ClientSideException | IOException cse) {
				throw ExceptionUtils.recastClientSideExceptionToServiceFailure(cse);
			}
    	}
    	return cn;
    }



    /**
     * Return an MNode using the nodeReference for the member node.  
     * 
     * @param nodeRef
     * @return
     * @throws ServiceFailure
     */
    public static MNode getMN(NodeReference nodeRef) throws ServiceFailure
    {
        bestAttemptRefreshNodeLocator();
        try {
            if (nodeLocator != null) {
                return  (MNode) nodeLocator.getNode(nodeRef);
            } else {
                throw new ServiceFailure("000", "Could not initialize the NodeLocator!");
            }
        } catch (ClientSideException e) {
            throw ExceptionUtils.recastClientSideExceptionToServiceFailure(e);
        }
    }
    

    private static synchronized void bestAttemptRefreshNodeLocator() {
        
        long now = System.currentTimeMillis();
        if (nodeLocator == null || now - lastNLRefresh > NODELOCATOR_STALE_INTERVAL) {
            if (now - lastNLRefreshAttempt > NODELOCATOR_REFRESH_ATTEMPT_INTERVAL) 
                // don't want to retry immediately after failure
                try { 
                    lastNLRefreshAttempt = System.currentTimeMillis();
                    NodeLocator newLocator = new SettingsContextNodeLocator(getMultipartRestClient()); 
                    nodeLocator = newLocator;
                    lastNLRefresh = System.currentTimeMillis();
                } catch (Throwable e1)  {
                    // keep the old one, but maybe warn?
                    logger.warn("Could not refresh D1Client's NodeLocator, using previous one.", e1);
                }
        }
    }

}
