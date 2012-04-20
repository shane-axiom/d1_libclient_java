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

package org.dataone.client.auth;

import java.io.IOException;
import java.net.URL;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.util.Constants;
import org.jibx.runtime.JiBXException;


/**
 * A client specific class to wrap certain CertificateManager methods, provide
 * some introspection on the current client identity, as well as methods to
 * manipulate identities.
 * 
 * @author rnahf
 *
 */
public class ClientIdentityManager {

	protected static Log log = LogFactory.getLog(ClientIdentityManager.class);
	
	/**
	 * a simple encapsulation to return the client's (current) identity
	 * from the CertificateManager as a dataone Subject 
	 * @return
	 */
	public static Subject getCurrentIdentity() 
	{
		CertificateManager cm = CertificateManager.getInstance();
		java.security.cert.X509Certificate x509cert = cm.loadCertificate();
		String subjectDN = cm.getSubjectDN(x509cert);
		
		Subject subject = new Subject();
		if (subjectDN != null) {
			subject.setValue(subjectDN);
		} else {
			subject.setValue(Constants.SUBJECT_PUBLIC);
		}
		return subject;
	}
	
	/**
	 * a simple encapsulation to return a session object built from the 
	 * CertificateManager
	 * 
	 * @return
	 * @throws JiBXException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws IOException 
	 */
	public static Session getCurrentSession() throws IOException, InstantiationException, IllegalAccessException, JiBXException 
	{
		CertificateManager cm = CertificateManager.getInstance();
		java.security.cert.X509Certificate x509cert = cm.loadCertificate();
		String subjectDN = cm.getSubjectDN(x509cert);
		
		Session session = new Session();
		
		Subject subject = new Subject();
		if (subjectDN != null) {
			subject.setValue(subjectDN);
			session.setSubject(subject);
			session.setSubjectInfo(cm.getSubjectInfo(x509cert));
		} 
		else {
			subject.setValue(Constants.SUBJECT_PUBLIC);
			session.setSubject(subject);	
		}
		return session;
	}
	
	/**
	 * uses the value of the property passed to setup the
	 * CertificateManager to use the certificate found at that path
	 * @return
	 */
	public static Subject setCurrentIdentity(String certificatePath) 
	{		
		URL url = ClientIdentityManager.class.getClassLoader().getResource(certificatePath);
		CertificateManager.getInstance().setCertificateLocation(url.getPath());
		Subject subject =  ClientIdentityManager.getCurrentIdentity();
		log.info("client setup as Subject: " + subject.getValue());
		return subject;
	}
	
	public static Date getCertificateExpriation()
	{
		java.security.cert.X509Certificate x509cert = CertificateManager.getInstance().loadCertificate();
		Date expires = x509cert.getNotAfter();
		return expires;
	}
}
