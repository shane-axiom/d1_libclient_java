package org.dataone.client.auth;

import java.net.URL;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.util.Constants;


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
		Subject subject = new Subject();
		java.security.cert.X509Certificate x509cert = CertificateManager.getInstance().loadCertificate();
		String subjectDN = CertificateManager.getInstance().getSubjectDN(x509cert);
		if (subjectDN != null) {
			subject.setValue(subjectDN);
		} else {
			subject.setValue(Constants.SUBJECT_PUBLIC);
		}
		return subject;
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
