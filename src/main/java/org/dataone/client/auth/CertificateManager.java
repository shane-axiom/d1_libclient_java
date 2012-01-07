package org.dataone.client.auth;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.bouncycastle.asn1.ASN1InputStream;
import org.bouncycastle.asn1.DERObject;
import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.DERUTF8String;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMReader;
import org.bouncycastle.openssl.PasswordFinder;
import org.dataone.client.CNode;
import org.dataone.client.D1Client;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SubjectInfo;
import org.dataone.service.util.TypeMarshaller;
import org.jibx.runtime.JiBXException;

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
    
    private static String CILOGON_OID_SUBJECT_INFO = null;

    private static CertificateManager cm = null;
    
    // keep track of the certs and private keys
    private Map<String, X509Certificate> certificates;
    
    private Map<String, PrivateKey> keys;
    
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
     * CertificateManager is normally a singleton, but custom instances can be created if needed.
     * Normally, getInstance() will provide the appropriate interface for certificate management.
     * In cases where the same process needs to act as multiple identities, new custom instances should
     * be created.
     */
    public CertificateManager() {
    	try {
	    	keyStorePassword = Settings.getConfiguration().getString("certificate.keystore.password");
	    	keyStoreType = Settings.getConfiguration().getString("certificate.keystore.type", KeyStore.getDefaultType());
	    	certificates = new HashMap<String, X509Certificate>();
	    	keys = new HashMap<String, PrivateKey>();
	    	
	    	CILOGON_OID_SUBJECT_INFO = Settings.getConfiguration().getString("cilogon.oid.subjectinfo", "1.3.6.1.4.1.34998.2.1");

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

    /**
     * Use this method to set the certificate to point CertificateManager to
     * a certificate at the designated file-path.  (Call before getKeyStore()
     * or getSSLSocketFactory()
     * @param certificate
     */
	public void setCertificateLocation(String certificate) {
		this.certificateLocation = certificate;
	}


	/**
	 * Track certificates and keys by subject 
	 * @param subject
	 * @param certificate
	 * @param key
	 */
	public void registerCertificate(String subject, X509Certificate certificate, PrivateKey key) {
		certificates.put(subject, certificate);
		keys.put(subject, key);
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
     * Load the configured certificate into the keystore singleton
     * Follows the logic of first searching the certificate at the setCertificateLocation()
     * location, then using the default location.
	 * 
     * @return the loaded X.509 certificate
     */
    public X509Certificate loadCertificate() {

        X509Certificate cert = null;
        try {
        	// load up the PEM
        	KeyStore keyStore = getKeyStore(null);
        	// get it from the store
            cert = (X509Certificate) keyStore.getCertificate("cilogon");
        } catch (Exception e) {
        	log.error(e.getMessage(), e);
        }
        return cert;
    }

    
    /**
     * Load configured private key from the keystore 
     */
    public PrivateKey loadKey() {

    	PrivateKey key = null;
        try {
        	// load up the PEM
        	KeyStore keyStore = getKeyStore(null);
        	// get it from the store
            key = (PrivateKey) keyStore.getKey("cilogon", keyStorePassword.toCharArray());
        } catch (Exception e) {
        	log.error(e.getMessage(), e);
        }
        return key;
    }
    
    /**
     * Retrieves the extension value given by the OID
     * @see http://stackoverflow.com/questions/2409618/how-do-i-decode-a-der-encoded-string-in-java 
     * @param X509Certificate
     * @param oid
     * @return
     * @throws IOException
     */
    protected String getExtensionValue(X509Certificate X509Certificate, String oid) throws IOException {
        String decoded = null;
        byte[] extensionValue = X509Certificate.getExtensionValue(oid);
        if (extensionValue != null) {
            DERObject derObject = toDERObject(extensionValue);
            if (derObject instanceof DEROctetString) {
                DEROctetString derOctetString = (DEROctetString) derObject;
                derObject = toDERObject(derOctetString.getOctets());
                if (derObject instanceof DERUTF8String) {
                    DERUTF8String s = DERUTF8String.getInstance(derObject);
                    decoded = s.getString();
                }
            }
        }
        return decoded;
    }

    /**
     * Converts the byte data into a DERObject
     * @see http://stackoverflow.com/questions/2409618/how-do-i-decode-a-der-encoded-string-in-java
     * @param data
     * @return
     * @throws IOException
     */
    private DERObject toDERObject(byte[] data) throws IOException {
        ByteArrayInputStream inStream = new ByteArrayInputStream(data);
        ASN1InputStream asnInputStream = new ASN1InputStream(inStream);
        return asnInputStream.readObject();
    }
    
    /**
     * Retrieve the SubjectInfo contained in the given certificate
     * @param certificate
     * @return subjectInfo from DataONE representing subject of the certificate 
     * @throws IOException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws JiBXException
     */
    public SubjectInfo getSubjectInfo(X509Certificate certificate) throws IOException, InstantiationException, IllegalAccessException, JiBXException {
    	String subjectInfoValue = this.getExtensionValue(certificate, CILOGON_OID_SUBJECT_INFO);
    	SubjectInfo subjectInfo = null;
    	if (subjectInfoValue != null) {
    		subjectInfo = TypeMarshaller.unmarshalTypeFromStream(SubjectInfo.class, new ByteArrayInputStream(subjectInfoValue.getBytes("UTF-8")));
    	}
    	return subjectInfo;
    }
    
    /**
     * Returns the RFC2253 string representation for the certificate's subject
     * This is the standard format used in DataONE.
     * @param certificate
     * @return subject DN using RFC2253 format
     */
    public String getSubjectDN(X509Certificate certificate) {
    	return certificate.getSubjectX500Principal().getName(X500Principal.RFC2253);
    }
    
    /**
     * Returns D1-wide consistent Subject DN string representations
     * @param name - the [reasonable] DN representation
     * @return the standard D1 representation
     */
    public String standardizeDN(String name) {
    	X500Principal principal = new X500Principal(name);
		String standardizedName = principal.getName(X500Principal.RFC2253);
		return standardizedName;
    }
    
    /**
     * Compare Subject DNs for equality
     * @param dn1 the DN representation
     * @param dn2 the other DN representation
     * @return the true if they are "equal"
     */
    public boolean equalsDN(String dn1, String dn2) {
    	// compare the standardized DNs
		return CertificateManager.getInstance().standardizeDN(dn1).equals(
				CertificateManager.getInstance().standardizeDN(dn2));
    }
    
    /**
     * Check the validity of a certificate, and be sure that it is verifiable using the given CA certificate.
     * @param cert the X509Certificate to be verified
     * @param caCert the X509Certificate of the trusted CertificateAuthority (CA)
     */
    public static boolean verify(X509Certificate cert, X509Certificate caCert) {
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
 
    /**
     * extracts the principal from the certificate passed in with the request 
     * and creates the dataone Session object.  The SubjectList is not filled out.
     *  
     * @param request
     * @return
     */
    public Session getSession(HttpServletRequest request) 
    {
    	return getSession(request,false);
    }
    
    /**
     * extracts the principal from the certificate passed in with the request
     * and creates the dataone Session object.
     * If lookupSubject parameter is true, an http call to the subject authority
     * is made (the CNIdentity service) to populate the SubjectList session property
     *  
     * @param request
     * @param lookupSubject - set to true to fill out the subject list from the
     *                            CNIdentity service
     * @return
     */
    public Session getSession(HttpServletRequest request, boolean lookupSubject) {
    	Session session = null;
    	
		X509Certificate x509Cert = getCertificate(request);
		if (x509Cert != null) {
			String subjectDN = getSubjectDN(x509Cert);
			Subject subject = new Subject();
			subject .setValue(subjectDN);
			session = new Session();
			session.setSubject(subject);

    		// look up the subject information from the CNIdentity service
    		if (lookupSubject) {
	    		try {
		    		CNode cn = D1Client.getCN();
					SubjectInfo subjectInfo = cn.getSubjectInfo(session, subject);
					session.setSubjectInfo(subjectInfo);
				} catch (Exception e) {
					// TODO: should we throw an exception/fail if this part fails?
					log.error("Could not retrieve complete Subject info for: " + subject, e);
				}
    		}
    	}
    	return session;
    }
    
    /**
     * Get the client certificate from the request object
     * @param request
     * @return
     */
    public X509Certificate getCertificate(HttpServletRequest request) {
    	Object certificate = request.getAttribute("javax.servlet.request.X509Certificate");
    	log.debug("javax.servlet.request.X509Certificate " + " = " + certificate);
    	Object sslSession = request.getAttribute("javax.servlet.request.ssl_session");
    	log.debug("javax.servlet.request.ssl_session " + " = " + sslSession);
    	if (certificate instanceof X509Certificate[]) {
    		X509Certificate[] x509Certificates = (X509Certificate[]) certificate;
    		for (X509Certificate x509Cert: x509Certificates) {
	    		displayCertificate(x509Cert);
	    		return x509Cert;
    		}
    	}
    	return null;
    }
    
    /**
     * For use by clients making requests via SSL connection.
     * Prepares and returns an SSL socket factory loaded with the certificate
     * determined by the subjectString.  
     * If the subjectString parameter is null, finds the certificate first using the 
     * set certificate location, and then the default location.
     * @param subjectString - used to determine which certificate to use for the connection
     *                        if null, finds the certificate first using the set certificate location,
     *                        and then the default location.
     * @return an SSLSockectFactory object configured with the specified certificate
     * @throws NoSuchAlgorithmException
     * @throws UnrecoverableKeyException
     * @throws KeyStoreException
     * @throws KeyManagementException
     * @throws CertificateException
     * @throws IOException
     */
    public SSLSocketFactory getSSLSocketFactory(String subjectString) 
    throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException,
    KeyManagementException, CertificateException, IOException 
    {
    	// our return object
    	log.debug("Enter getSSLSocketFactory");
    	SSLSocketFactory socketFactory = null;
    	KeyStore keyStore = null;
    	
    	// get the keystore that will provide the material
    	// Catch the exception here so that the TLS connection scheme
    	// will still be setup if the client certificate is not found.
    	try {
    		keyStore = getKeyStore(subjectString);
		} catch (FileNotFoundException e) {
			// these are somewhat expected for anonymous d1 client use
			log.warn("Could not set up client side authentication - likely because the certificate could not be located: " + e.getMessage());
		}
       
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
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());
        KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();
        
        // initialize the context
        ctx.init(keyManagers, new TrustManager[]{tm}, new SecureRandom());
        socketFactory = new SSLSocketFactory(ctx, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
        
        return socketFactory;
    }
    
    /**
     * Loads the certificate and privateKey into the in-memory KeyStore singleton, 
     * using the provided subjectString to search among the registered certificates.
     * If the subjectString parameter is null, finds the certificate first using the 
     * set certificate location, and then the default location. 
     *  
     * 
     * NOTE: this implementation uses Bouncy Castle security provider
     * 
     * @param subjectString - key for the registered certificate to load into the keystore
     *                        an unregistered subjectString will lead to a KeyStoreException
     *                        ("Cannot store non-PrivateKeys")
     * @return the keystore singleton instance that will provide the material
     * @throws KeyStoreException 
     * @throws CertificateException 
     * @throws NoSuchAlgorithmException 
     * @throws IOException 
     */
    private KeyStore getKeyStore(String subjectString) throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
    	
    	// the important items
    	X509Certificate certificate = null;
        PrivateKey privateKey = null;
        
    	// if we have a session subject, find the registered certificate and key
    	if (subjectString != null) {
    		certificate = certificates.get(subjectString);
    		privateKey = keys.get(subjectString);
    	}
    	else {
			// if the location has been set, use it
	    	String certLoc = certificateLocation;
	    	if (certLoc == null) {
	    		certLoc = locateCertificate();
	    	}
	        // get the private key and certificate from the PEM
	        // TODO: find a way to do this with default Java provider (not Bouncy Castle)?
	    	Security.addProvider(new BouncyCastleProvider());
	        PEMReader pemReader = new PEMReader(new FileReader(certLoc));
	        Object pemObject = null;
	        
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
    	}
    	
    	KeyStore keyStore  = KeyStore.getInstance(keyStoreType);
        keyStore.load(null, keyStorePassword.toCharArray());
        Certificate[] chain = new Certificate[] {certificate};
        // set the entry
		keyStore.setKeyEntry("cilogon", privateKey, keyStorePassword.toCharArray(), chain);
        
        return keyStore;
    	
    }
    
    /**
     * Load PrivateKey object from given file
     * @param fileName
     * @return
     * @throws IOException
     */
    public PrivateKey loadPrivateKeyFromFile(String fileName, final String password) throws IOException {
		
        // get the private key and certificate from the PEM
    	Security.addProvider(new BouncyCastleProvider());
        Object pemObject = null;
        PrivateKey privateKey = null;
        
        // is there a password for the key?
        PEMReader pemReader = null;
        if (password != null && password.length() > 0) {
        	PasswordFinder passwordFinder = new PasswordFinder() {
				@Override
				public char[] getPassword() {
					return password.toCharArray();
				}
        	};
			pemReader = new PEMReader(new FileReader(fileName), passwordFinder );
        } else {
        	pemReader = new PEMReader(new FileReader(fileName));
        }
        
        KeyPair keyPair = null;
        while ((pemObject = pemReader.readObject()) != null) {
			if (pemObject instanceof PrivateKey) {
				privateKey = (PrivateKey) pemObject;
				break;
			}
			else if (pemObject instanceof KeyPair) {
				keyPair = (KeyPair) pemObject;
				privateKey = keyPair.getPrivate();
				break;
			}
		}
        return privateKey;
    }


    /**
     * Load X509Certificate object from given file
     * @param fileName
     * @return
     * @throws IOException
     */
    public X509Certificate loadCertificateFromFile(String fileName) throws IOException {
		X509Certificate certificate = null;
		
    	// get the private key and certificate from the PEM
    	Security.addProvider(new BouncyCastleProvider());
        PEMReader pemReader = new PEMReader(new FileReader(fileName));
        Object pemObject = null;
        
        while ((pemObject = pemReader.readObject()) != null) {
			if (pemObject instanceof X509Certificate) {
				certificate = (X509Certificate) pemObject;
				break;
			}
		}
        return certificate;
    }
    
    /**
     * Locate the default certificate location
     * http://www.cilogon.org/cert-howto#TOC-Finding-CILogon-Certificates
     * @return
     * @throws FileNotFoundException 
     */
    private String locateCertificate() throws FileNotFoundException {
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
		File fileLocation = new File(location.toString());
		if (!fileLocation.exists()) {
			throw new FileNotFoundException("No certificate installed in expected location: " + location.toString());
		}
		
    	return location.toString();
    }
    
    /**
     * Show details of an X509 certificate, printing the information to STDOUT.
     * @param cert the certificate to be displayed
     */
    public void displayCertificate(X509Certificate cert) {
        if (cert == null) {
            return;
        }
        if (log.isDebugEnabled()) {
        	log.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

        	log.debug(" Issuer: " + cert.getIssuerX500Principal().getName(X500Principal.RFC2253));
        	Date notBefore = cert.getNotBefore(); 
        	DateFormat fmt = SimpleDateFormat.getDateTimeInstance();
        	log.debug("   From: " + fmt.format(notBefore));
        	Date notAfter = cert.getNotAfter();
        	log.debug("     To: " + fmt.format(notAfter));
        	log.debug("Subject: " + getSubjectDN(cert));
        	//        Principal subjectDN = cert.getSubjectDN();
        	//        log.debug("Subject Name: " + subjectDN.getName());
        	//        log.debug("Subject x500 Principal default: " + cert.getSubjectX500Principal().getName());
        	//        log.debug("Subject x500 Principal CANONICAL: " + cert.getSubjectX500Principal().getName(X500Principal.CANONICAL));
        	//        log.debug("Subject x500 Principal RFC1779: " + cert.getSubjectX500Principal().getName(X500Principal.RFC1779));
        	//        log.debug("Subject x500 Principal RFC2253: " + cert.getSubjectX500Principal().getName(X500Principal.RFC2253));
        	log.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        }
    }
}
