package org.dataone.client.auth;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMReader;
import org.dataone.client.CNode;
import org.dataone.client.D1Client;
import org.dataone.configuration.Settings;
import org.dataone.service.types.Session;
import org.dataone.service.types.Subject;
import org.dataone.service.types.SubjectList;

/**
 * Import and manage certificates to be used for authentication against DataONE
 * service providers.  This class is a singleton, as in any given application there 
 * need only be one collection of certificates.  
 * @author Matt Jones, Ben Leinfelder
 */
public class CertificateManager {
	
	private static Log log = LogFactory.getLog(CertificateManager.class);
	
	// this can be set by caller if the default discovery mechanism is not applicable
	private String certificateLocation = null;
	
	// other variables
	private String keyStorePassword = null;
	private String keyStoreType = null;

    // this is packaged with the library
    private static final String caTrustStore = "cilogon-trusted-certs";
    private static final String caTrustStorePass = "cilogon";

    private static CertificateManager cm = null;
    
    /*
     * Some useful links to background info:
     * 
     * About how to do cert validation:
     * http://osdir.com/ml/users-tomcat.apache.org/2009-10/msg01160.html
     * 
     * About how to create X.509 certs in Java:
     * http://www.mayrhofer.eu.org/create-x509-certs-in-java
     */
    
    /**
     * CertificateManager is a singleton, so use getInstance() to get it.
     */
    private CertificateManager() {
    	try {
	    	keyStorePassword = Settings.getConfiguration().getString("certificate.keystore.password");
	    	keyStoreType = Settings.getConfiguration().getString("certificate.keystore.type", KeyStore.getDefaultType());
    	} catch (Exception e) {
            log.error(e.getMessage(), e);
		}
    
    }
    
    /**
     * Return the singleton instance of this CertificateManager, creating it if needed.
     * @return an instance of CertificateManager
     */
    public static CertificateManager getInstance() {
        if (cm == null) {
            cm = new CertificateManager();
        }
        return cm;
    }
    
    public String getCertificateLocation() {
		return certificateLocation;
	}

	public void setCertificateLocation(String certificate) {
		this.certificateLocation = certificate;
	}


	/**
     * Find the CA certificate to be used to validate user certificates.
     * @return X509Certificate for the root CA
     */
    public X509Certificate getCACert(String caAlias) {
        X509Certificate caCert = null;
        KeyStore trustStore = null;
        try {
            trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            InputStream caStream = this.getClass().getResourceAsStream(caTrustStore);
            trustStore.load(caStream, caTrustStorePass.toCharArray());
            caCert = (X509Certificate) trustStore.getCertificate(caAlias);
        } catch (KeyStoreException e) {
            log.error(e.getMessage(), e);
        } catch (NoSuchAlgorithmException e) {
        	log.error(e.getMessage(), e);
        } catch (CertificateException e) {
        	log.error(e.getMessage(), e);
        } catch (FileNotFoundException e) {
        	log.error(e.getMessage(), e);
        } catch (IOException e) {
        	log.error(e.getMessage(), e);
        }
        return caCert;
    }
    
    /**
     * Load configured certificate from the keystore
     */
    public X509Certificate loadCertificate() {

        X509Certificate cert = null;
        try {
        	// load up the PEM
        	KeyStore keyStore = getKeyStore();
        	// get it from the store
            cert = (X509Certificate) keyStore.getCertificate("cilogon");
        } catch (Exception e) {
        	log.error(e.getMessage(), e);
        }
        return cert;
    }
    
    /**
     * Check the validity of a certificate, and be sure that it is verifiable using the given CA certificate.
     * @param cert the X509Certificate to be verified
     * @param caCert the X509Certificate of the trusted CertificateAuthority (CA)
     */
    public boolean verify(X509Certificate cert, X509Certificate caCert) {
        boolean isValid = false;
        try {
            cert.checkValidity();
            cert.verify(caCert.getPublicKey());
            isValid = true;
        } catch (CertificateException e) {
        	log.error(e.getMessage(), e);
        } catch (InvalidKeyException e) {
        	log.error("Certificate verification failed, invalid key.");
            log.error(e.getMessage(), e);
        } catch (NoSuchAlgorithmException e) {
        	log.error("Certificate verification failed, no such algorithm.");
            log.error(e.getMessage(), e);
        } catch (NoSuchProviderException e) {
        	log.error("Certificate verification failed, no such provider.");
            log.error(e.getMessage(), e);
        } catch (SignatureException e) {
        	log.error("Certificate verification failed, signatures do not match.");
        }
        return isValid;
    }
    
    public Session getSession(HttpServletRequest request) {
    	Session session = new Session();
		Subject subject = new Subject();
    	Object certificate = request.getAttribute("javax.servlet.request.X509Certificate");
    	log.debug("javax.servlet.request.X509Certificate " + " = " + certificate);
    	Object sslSession = request.getAttribute("javax.servlet.request.ssl_session");
    	log.debug("javax.servlet.request.ssl_session " + " = " + sslSession);
    	if (certificate instanceof X509Certificate[]) {
    		X509Certificate[] x509Certificates = (X509Certificate[]) certificate;
    		for (X509Certificate x509Cert:x509Certificates) {
	    		displayCertificate(x509Cert);
	    		Principal subjectDN = x509Cert.getSubjectDN();
	    		subject.setValue(subjectDN.toString());
	    		session.setSubject(subject);
	    		//TODO get the SubjectList info from certificate
	    		break;
    		}
    		
    		// look up the subject information from the CNIdentity service
    		boolean lookupSubject = false;
    		if (lookupSubject) {
	    		CNode cn = D1Client.getCN();
	    		try {
					SubjectList subjectList = cn.getSubjectInfo(session, subject);
					session.setSubjectList(subjectList);
				} catch (Exception e) {
					// TODO: should we throw an exception/fail if this part fails?
					log.error("Could not retrieve complete Subject info for: " + subject, e);
				}
    		}
    	}
    	return session;
    }
    
    public SSLSocketFactory getSSLSocketFactory() throws Exception{
    	// our return object
    	SSLSocketFactory socketFactory = null;
    	
    	// get the keystore that will provide the material
    	KeyStore keyStore = getKeyStore();
       
        // create SSL context
        SSLContext ctx = SSLContext.getInstance("TLS");
        
        // use a very liberal trust manager for trusting the server
        // TODO: check server trust policy
        X509TrustManager tm = new X509TrustManager() {
            public void checkClientTrusted(X509Certificate[] xcs, String string) throws CertificateException {
            	log.debug("checkClientTrusted - " + string);
            }
            public void checkServerTrusted(X509Certificate[] xcs, String string) throws CertificateException {
            	log.debug("checkServerTrusted - " + string);
            }
            public X509Certificate[] getAcceptedIssuers() {
            	log.debug("getAcceptedIssuers");
            	return null;
            }
        };
        
        // specify the client key manager
        KeyManager[] keyManagers = null;
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());
        keyManagers = keyManagerFactory.getKeyManagers();
        
        // initialize the context
        ctx.init(keyManagers, new TrustManager[]{tm}, new SecureRandom());
        socketFactory = new SSLSocketFactory(ctx);
        
        socketFactory.setHostnameVerifier(SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
        
        return socketFactory;
    }
    
    /**
     * Load PEM file contents into in-memory keystore
     * NOTE: this implementation uses Bouncy Castle security provider
     * @return the keystore that will provide the material
     */
    private KeyStore getKeyStore() {
    	
    	// if the location has been set, use it
    	String certLoc = certificateLocation;
    	if (certLoc == null) {
    		certLoc = locateCertificate();
    	}
    	KeyStore keyStore = null;
        try {
        	keyStore  = KeyStore.getInstance(keyStoreType);
            keyStore.load(null, keyStorePassword.toCharArray());

            // get the private key and certificate from the PEM
            // TODO: find a way to do this with default Java provider (not Bouncy Castle)?
        	Security.addProvider(new BouncyCastleProvider());
            PEMReader pemReader = new PEMReader(new FileReader(certLoc));
            Object pemObject = null;
            X509Certificate certificate = null;
            PrivateKey privateKey = null;
            KeyPair keyPair = null;
            while ((pemObject = pemReader.readObject()) != null) {
				if (pemObject instanceof PrivateKey) {
					privateKey = (PrivateKey) pemObject;
				}
				else if (pemObject instanceof KeyPair) {
					keyPair = (KeyPair) pemObject;
					privateKey = keyPair.getPrivate();
				} else if (pemObject instanceof X509Certificate) {
					certificate = (X509Certificate) pemObject;
				}
			}

            Certificate[] chain = new Certificate[] {certificate};
            
            // set the entry
			keyStore.setKeyEntry("cilogon", privateKey, keyStorePassword.toCharArray(), chain);
        } 
        catch (Exception e) {
        	log.error(e.getMessage(), e);
        }
        
        return keyStore;
    	
    }
    
    /**
     * Locate the default certificate location
     * http://www.cilogon.org/cert-howto#TOC-Finding-CILogon-Certificates
     * @return
     */
    private String locateCertificate() {
    	StringBuffer location = new StringBuffer();
    	
    	// the tmp dir
    	String tmp = System.getProperty("tmpdir");
    	if (tmp == null) {
    		tmp = "/tmp";
    	}

    	// UID
    	String uid = null;
    	try {
    		// get the user id from *nix systems
    		Process process = Runtime.getRuntime().exec("id -u");
    		int ret = process.waitFor();
    		if (ret == 0) {
    			InputStream stream = process.getInputStream();
    			BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
    			String result = reader.readLine();
    			// verify it is a number
    			int testUid = Integer.parseInt(result);
    			uid = String.valueOf(testUid);
    		}
    	} catch (Exception e) {
			log.warn("No UID found, using user.name");
		}
    	if (uid == null) {
    		uid = System.getProperty("user.name");
    	}
		location.append(tmp);
		location.append("/");
		location.append("x509up_u");
		location.append(uid);

		log.debug("Calculated certificate location: " + location.toString());
		
    	return location.toString();
    }
    
    /**
     * Show details of an X509 certificate, printing the information to STDOUT.
     * @param cert the certificate to be displayed
     */
    protected void displayCertificate(X509Certificate cert) {
        if (cert == null) {
            return;
        }
        log.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        Principal issuerDN = cert.getIssuerDN();
        log.debug(" Issuer: " + issuerDN.toString());
        Date notBefore = cert.getNotBefore();
        DateFormat fmt = SimpleDateFormat.getDateTimeInstance();
        log.debug("   From: " + fmt.format(notBefore));
        Date notAfter = cert.getNotAfter();
        log.debug("     To: " + fmt.format(notAfter));
        Principal subjectDN = cert.getSubjectDN();
        log.debug("Subject: " + subjectDN.toString());
        log.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
    }
}
