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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.bouncycastle.asn1.ASN1InputStream;
import org.bouncycastle.asn1.DERObject;
import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.DERUTF8String;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMReader;
import org.bouncycastle.openssl.PasswordFinder;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SubjectInfo;
import org.dataone.service.util.TypeMarshaller;
import org.jibx.runtime.JiBXException;

/**
 * Import and manage certificates to be used for authentication against DataONE
 * service providers.  It uses the concepts of 'keystores' and 'truststores' to 
 * represent, respectively, the client certificate to be used for SSL connections,
 * and the set of CA certificates that are trusted.
 * <br>
 * 
 * For the client keystore, the CertificateManager knows how to locate CILogon
 * certificates downloaded via a browser.  Because certificate downloads take place
 * outside of the application using this class, the client keystore is not cached.
 * <br>
 * 
 * For the truststore, the CertificateManager builds and caches a TrustManager that
 * is reused for all SSL connections it builds.  It is assumed that these certificate
 * are long-lived and as a whole, the set of them are stable throughout the life
 * of the application.  The TrustManager contains all of the CA certificates used
 * by the JVM, and defaults also to include DataONE-trusted CA certificate that 
 * ship with d1_libclient_java.jar.  For more information, see getSSLConnectionFactory()
 * <br>
 * 
 * This class is a singleton, as in any given application there 
 * need only be one collection of certificates.  
 * @author Matt Jones, Ben Leinfelder
 */
public class CertificateManager {
	
	static Log log = LogFactory.getLog(CertificateManager.class);
//	private static Log trustManLog = LogFactory.getLog(X509TrustManager.class);
	
	// this can be set by caller if the default discovery mechanism is not applicable
	private String certificateLocation = null;
	
	// other variables
	private String keyStorePassword = null;
	private String keyStoreType = null;

    // this is packaged with the library
    private static final String shippedCAcerts = "/org/dataone/client/auth/d1-trusted-certs.crt";
//    private static final char[] caTrustStorePass = "dataONE".toCharArray();
    private KeyStore d1TrustStore;
   
    // BouncyCastle added to be able to get the private key and certificate from the PEM
    // TODO: find a way to do this with default Java provider (not Bouncy Castle)? 
    static {
		Security.addProvider(new BouncyCastleProvider());
    }
    
    private static String CILOGON_OID_SUBJECT_INFO = null;

    private static CertificateManager cm = null;
    
    // keep track of the certs and private keys
    private Map<String, X509Certificate> certificates;
    
    private Map<String, PrivateKey> keys;

	private boolean trustStoreIncludesD1CAs = true;
    
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
	    	keyStorePassword = Settings.getConfiguration().getString("certificate.keystore.password", "changeit");
	    	keyStoreType = Settings.getConfiguration().getString("certificate.keystore.type", KeyStore.getDefaultType());
	    	trustStoreIncludesD1CAs = Settings.getConfiguration().getBoolean("certificate.truststore.includeD1CAs", true);
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
	 * Register certificates to be used by getSSLSocjetFactory(subject) for setting
	 * up connections, using the subject as the lookup key
	 * @param subject
	 * @param certificate
	 * @param key
	 */
	public void registerCertificate(String subject, X509Certificate certificate, PrivateKey key) {
		certificates.put(subject, certificate);
		keys.put(subject, key);
	}

	
	
	/**
	 * this method builds the truststore from the proper file, first looking
	 * for the one at the auxiliary location, then defaulting to the one shipped
	 * with libclient_java.  The idea that updates to the dataone-trusted lists
	 * should be used first, if they exist.
	 */
	// TODO: it would be best to have the aux.location be a standard predefined location,
	// so that multiple libclient-using applications can get the update.
	private KeyStore loadTrustStore()
	{
		if (this.d1TrustStore == null ) {
			try {
				this.d1TrustStore = KeyStore.getInstance(KeyStore.getDefaultType());
			    this.d1TrustStore.load(null, null);
				
				
				String auxLocation = Settings.getConfiguration().getString("certificate.truststore.aux.location");
				
				int count = 0;
				if (auxLocation != null) {
					File loc = new File(auxLocation);
					if (loc.exists()) {
						if (loc.isDirectory()) {
							for (File f : loc.listFiles()) {
								count += loadIntoTrustStore(this.d1TrustStore, new FileReader(f.getAbsolutePath()));
							}
						} 
						else {
							count += loadIntoTrustStore(this.d1TrustStore, new FileReader(loc.getAbsolutePath()));
						}
					}
				}
				if (count == 0) {
					InputStream shippedCerts = this.getClass().getResourceAsStream(shippedCAcerts);
					if (shippedCerts != null) {
						count += loadIntoTrustStore(this.d1TrustStore, new InputStreamReader(shippedCerts));
					} else {
						log.error("'shippedCAcerts' file (" + shippedCAcerts + 
								  ") could not be found. No DataONE-trusted CA certs loaded");
					}
				}
				if (log.isDebugEnabled()) {
					Enumeration<String> aliases = this.d1TrustStore.aliases();
					while (aliases.hasMoreElements()) {
						log.debug(aliases.nextElement());
					}
					log.debug(this.d1TrustStore.aliases());
				}
			} catch (KeyStoreException e) {
				log.error(e.getMessage(), e);
			} catch (FileNotFoundException e) {
				log.error(e.getMessage(), e);
			} catch (NoSuchAlgorithmException e) {
				log.error(e.getMessage(), e);
			} catch (CertificateException e) {
				log.error(e.getMessage(), e);
			} catch (IOException e) {
				log.error(e.getMessage(), e);
			} 
		}
	
        return this.d1TrustStore;
	}
	
	
	private int loadIntoTrustStore(KeyStore trustStore, Reader certLoc) 
	throws FileNotFoundException
	{
    	int count = 0;
		PEMReader pemReader = null;
    	try {
    		pemReader = new PEMReader(certLoc);
    		
    		Object pemObject;
			log.info("loading into client truststore: ");
    		while ((pemObject = pemReader.readObject()) != null) {
    			if (pemObject instanceof X509Certificate) {
    				X509Certificate certificate = (X509Certificate) pemObject;
    				String alias = certificate.getSubjectX500Principal().getName();
    				
    				if (!trustStore.containsAlias(alias)) {
    					log.info(count + " alias " + alias);
    					trustStore.setCertificateEntry(alias, certificate);
    					count++;
    				}
    			}
    		}
    	} catch (KeyStoreException e) {
    		log.error(e.getMessage() + " after loading " + count + " certificates", e);
		} catch (IOException e) {
			log.error(e.getMessage() + " after loading " + count + " certificates", e);
		} finally {
    		IOUtils.closeQuietly(pemReader);
    	}
    	return count;
	}
	
	/**
     * Find the supplemental CA certificate to be used to validate user (peer) <
     * @return X509Certificate for the root CA
     */
    public X509Certificate getCACert(String caAlias) 
    {
        X509Certificate caCert = null;
        KeyStore trustStore = loadTrustStore();   
        try {
			caCert = (X509Certificate) trustStore.getCertificate(caAlias);
        } catch (KeyStoreException e) {
            log.error(e.getMessage(), e);
		}
        return caCert;
    }
    
    /**
     * Find all supplemental CA certificates to be used to validate peer certificates.
     * 
     * @return List<X509Certificate> of CAs
     */
    private List<X509Certificate> getSupplementalCACertificates() {
    	List<X509Certificate> caCerts = null;
    	InputStream caStream = null;
        try {
            KeyStore trustStore = loadTrustStore();
            Enumeration<String> aliases = trustStore.aliases();
        	caCerts = new ArrayList<X509Certificate>();
            while (aliases.hasMoreElements()) {
            	String caAlias = aliases.nextElement();
                X509Certificate caCert = (X509Certificate) trustStore.getCertificate(caAlias);
                caCerts.add(caCert);
            }
        } catch (KeyStoreException e) {
            log.error(e.getMessage(), e);
        } finally {
        	IOUtils.closeQuietly(caStream);
        }
        return caCerts;
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
        } catch (FileNotFoundException fnf) {
        	log.warn(fnf.getMessage());
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
        
    	DERObject dero = null;
    	ASN1InputStream asnInputStream = null;
    	try {
        	ByteArrayInputStream inStream = new ByteArrayInputStream(data);
        	asnInputStream = new ASN1InputStream(inStream);
        	dero = asnInputStream.readObject();
    	} finally {
    		IOUtils.closeQuietly(asnInputStream);
    	}
        return dero;
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
    public SubjectInfo getSubjectInfo(X509Certificate certificate) 
    throws IOException, InstantiationException, IllegalAccessException, JiBXException 
    {
    	String subjectInfoValue = this.getExtensionValue(certificate, CILOGON_OID_SUBJECT_INFO);
    	log.debug("Certificate subjectInfoValue: " + subjectInfoValue);
    	SubjectInfo subjectInfo = null;
    	if (subjectInfoValue != null) {
    		subjectInfo = TypeMarshaller.unmarshalTypeFromStream(SubjectInfo.class, 
    				new ByteArrayInputStream(subjectInfoValue.getBytes("UTF-8")));
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
    	if (certificate == null) {
    		return null;
    	}
    	X500Principal principal = certificate.getSubjectX500Principal();
    	String dn = principal.getName(X500Principal.RFC2253);
    	//dn = standardizeDN(dn);
    	return dn;
    }
    
    /**
     * Returns D1-wide consistent Subject DN string representations
     * @see http://www.ietf.org/rfc/rfc2253.txt
     * @param name - the [reasonable] DN representation
     * @return the standard D1 representation
     */
    public String standardizeDN(String name) {
		log.debug("name: " + name);
    	X500Principal principal = new X500Principal(name);
    	String standardizedName = principal.getName(X500Principal.RFC2253);
		log.debug("standardizedName: " + standardizedName);
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
    
    public static boolean verify(X509Certificate cert, X509Certificate caCert)
    {
    	return verify(cert, caCert, true);
    }
    
    
    /**
     * Check the validity of a certificate, and be sure that it is verifiable using the given CA certificate.
     * @param cert the X509Certificate to be verified
     * @param caCert the X509Certificate of the trusted CertificateAuthority (CA)
     */
    public static boolean verify(X509Certificate cert, X509Certificate caCert, boolean logExceptions) {
        boolean isValid = false;
        try {
            cert.checkValidity();
            cert.verify(caCert.getPublicKey());
            isValid = true;
        } catch (CertificateException e) {
        	if (logExceptions)
        		log.error(e.getMessage(), e);
        } catch (InvalidKeyException e) {
        	if (logExceptions) {
        		log.error("Certificate verification failed, invalid key.");
        		log.error(e.getMessage(), e);
        	}
        } catch (NoSuchAlgorithmException e) {
        	if (logExceptions) {
        		log.error("Certificate verification failed, no such algorithm.");
            	log.error(e.getMessage(), e);
        	}
        } catch (NoSuchProviderException e) {
        	if (logExceptions) {
        		log.error("Certificate verification failed, no such provider.");
            	log.error(e.getMessage(), e);
        	}
        } catch (SignatureException e) {
        	if (logExceptions)
        		log.error("Certificate verification failed, signatures do not match.");
        }
        return isValid;
    }
    
    /**
     * extracts the principal from the certificate passed in with the request
     * and creates the dataone Session object.
     *  
     * @param request

     * @return
     * @throws InvalidToken 
     */
    public Session getSession(HttpServletRequest request) throws InvalidToken {
    	Session session = null;
    	
		X509Certificate x509Cert = getCertificate(request);
		if (x509Cert != null) {
			String subjectDN = getSubjectDN(x509Cert);
			Subject subject = new Subject();
			subject .setValue(subjectDN);
			session = new Session();
			session.setSubject(subject);
			
			
			SubjectInfo subjectInfo = null;
			try {
				subjectInfo = getSubjectInfo(x509Cert);
			} catch (Exception e) {
				// throw an InvalidToken
				String msg = "Could not get SubjectInfo from certificate for: " + subject.getValue();
				log.error(msg, e);
				throw new InvalidToken("", msg);
			}
    		
    		// set in the certificate
			session.setSubjectInfo(subjectInfo);
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
     * It prepares and returns an SSL socket factory loaded with the certificate
     * determined by the subjectString (see parameter details).
     * <br>
     * It also configures a TrustManager that uses a supplemental trust-store
     * of DataONE trusted CAs in addition to the default set of CAs registered
     * to the local system (exposed via Java Security).
     * <br>
     * One can turn off the supplemental DataONE trusted CAs by setting the property
     * 'certificate.truststore.includeD1CAs=false'
     * <br>
     * The process of loading DataONE-trusted CA certificates is first to try to 
     * find them in a default auxiliary location determined by 
     * 'certificate.truststore.aux.location'.  Failing to find either the directory
     * or any certificates in the directory, it will load a set of CA certificates
     * that ship with d1_libclient_java.  This auxiliary location is to allow libclient
     * applications to keep up with any updates to the trust-list without having
     * to update libclient_java itself.  Please note, however, that the CA certificates
     * that ship with libclient_java will not be loaded if any certificates are 
     * found in the auxiliary location (it uses one or the other)
     * 
     * @param subjectString - used to determine which certificate to use for the connection.
     *                        If null, it auto-discovers the certificate, using the setCertificate()
     *                        location, (if not set, uses the default location)
     *                        Otherwise, looks up the certificate from among those registered
     *                        with registerCertificate().
     * @return an SSLSockectFactory object configured with the specified certificate
     * @throws NoSuchAlgorithmException
     * @throws UnrecoverableKeyException
     * @throws KeyStoreException - thrown if an unknown subject value provided
     * @throws KeyManagementException
     * @throws CertificateException
     * @throws IOException
     */
    public SSLSocketFactory getSSLSocketFactory(String subjectString) 
    throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException,
    KeyManagementException, CertificateException, IOException 
    {
    	// our return object
    	log.info("Entering getSSLSocketFactory");
    	SSLSocketFactory socketFactory = null;
    	KeyStore keyStore = null;
    	
    	// get the keystore that will provide the material
    	// Catch the exception here so that the TLS connection scheme
    	// will still be setup if the client certificate is not found.
    	try {
    		keyStore = getKeyStore(subjectString);
		} catch (FileNotFoundException e) {
			// these are somewhat expected for anonymous d1 client use
			log.warn("Client certificate could not be located. Setting up SocketFactory without it." + e.getMessage());
		}
       
        // create SSL context
        SSLContext ctx = SSLContext.getInstance("TLS");
        
        // based on config options, we get an appropriate truststore
        X509TrustManager tm = getTrustManager();
        
        // specify the client key manager
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());
        KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();
        
        // initialize the context
        ctx.init(keyManagers, new TrustManager[]{tm}, new SecureRandom());
        if (trustStoreIncludesD1CAs) {
        	log.info("using allow-all hostname verifier");
	        //socketFactory = new SSLSocketFactory(ctx, SSLSocketFactory.STRICT_HOSTNAME_VERIFIER);
	        //socketFactory = new SSLSocketFactory(ctx, SSLSocketFactory.BROWSER_COMPATIBLE_HOSTNAME_VERIFIER);
	        socketFactory = new SSLSocketFactory(ctx, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
        } else {
	        socketFactory = new SSLSocketFactory(ctx);
        }
        
        return socketFactory;
    }
    
    
    
    
    public SSLConnectionSocketFactory getSSLConnectionSocketFactory(String subjectString) 
    	    throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException,
    	    KeyManagementException, CertificateException, IOException 
    	    {
    	    	// our return object
    	    	log.info("Entering getSSLConnectionSocketFactory");
    	    	SSLConnectionSocketFactory socketFactory = null;
    	    	KeyStore keyStore = null;
    	    	
    	    	// get the keystore that will provide the material
    	    	// Catch the exception here so that the TLS connection scheme
    	    	// will still be setup if the client certificate is not found.
    	    	try {
    	    		keyStore = getKeyStore(subjectString);
    			} catch (FileNotFoundException e) {
    				// these are somewhat expected for anonymous d1 client use
    				log.warn("Client certificate could not be located. Setting up SocketFactory without it." + e.getMessage());
    			}
    	       
    	        // create SSL context
    	        SSLContext ctx = SSLContext.getInstance("TLS");
    	        
    	        // based on config options, we get an appropriate truststore
    	        X509TrustManager tm = getTrustManager();
    	        
    	        // specify the client key manager
    	        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    	        keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());
    	        KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();
    	        
    	        // initialize the context
    	        ctx.init(keyManagers, new TrustManager[]{tm}, new SecureRandom());
    	        if (trustStoreIncludesD1CAs) {
    	        	log.info("using allow-all hostname verifier");
    		        //socketFactory = new SSLSocketFactory(ctx, SSLSocketFactory.STRICT_HOSTNAME_VERIFIER);
    		        //socketFactory = new SSLSocketFactory(ctx, SSLSocketFactory.BROWSER_COMPATIBLE_HOSTNAME_VERIFIER);
    		        socketFactory = new SSLConnectionSocketFactory(ctx, SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
    	        } else {
    		        socketFactory = new SSLConnectionSocketFactory(ctx);
    	        }
    	        
    	        return socketFactory;
    	    }
    
    /**
     * Based on the configuration option 'certificate.truststore.includeD1CAs', 
     * returns either the JVM trustmanager or a custom trustmanager that augments 
     * the JVM one with supplemental CA certificates for dataONE.
     * see loadTrustStore() for more details.
     * 
     * @return X509TrustManager for verifying server identity
     * @throws NoSuchAlgorithmException
     * @throws KeyStoreException
     */
    private X509TrustManager getTrustManager() throws NoSuchAlgorithmException, KeyStoreException {
    	
    	X509TrustManager tm = null;
    	
    	X509TrustManager jvmTrustManager = null;
    	
		// this is the Java truststore and is administered outside of the code
    	TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());  
	    trustManagerFactory.init((KeyStore) null);  
	      
	    log.debug("JVM Default Trust Managers:");  
	    for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {  
	        log.debug(trustManager);  
	        if (trustManager instanceof X509TrustManager) {  
	        	jvmTrustManager = (X509TrustManager) trustManager;  
	            log.debug("Accepted issuers count : " + jvmTrustManager.getAcceptedIssuers().length);
	            //for (X509Certificate issuer: x509TrustManager.getAcceptedIssuers()) {
	            //	log.debug("trusted issuer: " + issuer.getSubjectDN().toString());
	            //}
	            
	            // we will use the default
	            break;
	        }  
	    }
	
	    
	    // choose to use the default as is, or make an augmented trust manager with additional entries
	    // TODO: remove the System.out.println statements in the TrustManager
	    if (trustStoreIncludesD1CAs) {
	    	log.info("creating custom TrustManager");
	    	
		    // create a trustmanager from the default that is augmented with DataONE-trusted CAs
			final X509TrustManager defaultTrustManager = jvmTrustManager;
			tm = new X509TrustManager() {
				
				private List<X509Certificate> d1CaCertificates = getSupplementalCACertificates();
	
	            public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
	            
	            	// check DataONE-trusted CAs in addition to the default
	        		boolean trusted = false;
	        		List<X509Certificate> combinedIssuers = Arrays.asList(getAcceptedIssuers());
	        		for (X509Certificate cert: chain) {
	        			if (combinedIssuers.contains(cert)) {
	        				trusted = true;
	        				break;
	        			}
	        		}
	        		if (!trusted) {
	        			//throw new CertificateException("Certificate issuer not found in trusted CAs");
	        			// try the default which will succeed or throw an exception
	        			defaultTrustManager.checkClientTrusted(chain, authType);
	        		}	
	            }
	            
	            public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {

	            	// check DataONE-trusted CAs in addition to the default
	        		boolean trusted = false;
	        		List<X509Certificate> combinedIssuers = Arrays.asList(getAcceptedIssuers());
	        		for (X509Certificate cert: chain) {
	        			for (X509Certificate caCert : combinedIssuers) {
	        				if (CertificateManager.verify(cert, caCert, false)) {
	        					trusted = true;
	        					break;
	        				}
	        			}
	        		}
	        		if (!trusted) {
	        			//throw new CertificateException("Certificate issuer not found in trusted CAs");
	        			// try the default, which will either succeed, in which case we are good, or will throw exception
	        			try {
	        				defaultTrustManager.checkServerTrusted(chain, authType);
	        			} catch (CertificateException ce) {
	        				if (log.isTraceEnabled()) {
	        					log.trace("CertMan Custom TrustManager: server cert chain subjectDNs: ");
	        					for (X509Certificate cert: chain) {
	        						log.trace("CertMan Custom TrustManager:   subjDN: " + cert.getSubjectDN() + 
	        								" / issuerDN: " + cert.getIssuerX500Principal());
	        					}
	        				}
	        				throw ce;
	        			}
	        		}
	            }
	            
	            public X509Certificate[] getAcceptedIssuers() {
	            	List<X509Certificate> combinedIssuers = new ArrayList<X509Certificate>();
	            	// add DataONE-trusted CAs as accepted issuers, no matter what
	            	combinedIssuers.addAll(d1CaCertificates);
	            	// include the entries from the default
	            	combinedIssuers.addAll(Arrays.asList(defaultTrustManager.getAcceptedIssuers()));
	            	
	            	return combinedIssuers.toArray(new X509Certificate[0]);
	            }
	        };
	        

//  uncomment the following to create an all-trusting trust manager
//  WARNING: this is inherently unsafe, as you allow MITM attacks	        
//	        tm = new X509TrustManager() {
//				
//	            public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
//	            	;
//	            }
//	            
//	            public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
//	            	;
//	            }
//	            
//	            public X509Certificate[] getAcceptedIssuers() {
//	            	return null;
//	            }
//	        };
	        
	        
	        
	    }
	    else {
	    	log.info("using JVM TrustManager");
	    	tm = jvmTrustManager;
	    }

        return tm;

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
	    	File certFile = (certLoc == null) ? 
	    			locateDefaultCertificate() :
	    			new File(certLoc);

			log.info("Using client certificate location: " + certLoc);
	    	PEMReader pemReader = null;
	    	try {
	    		pemReader = new PEMReader(new FileReader(certFile));
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
	    	} finally {
	    		IOUtils.closeQuietly(pemReader);
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
		
        Object pemObject = null;
        PrivateKey privateKey = null;
        
        // is there a password for the key?
        PEMReader pemReader = null;
        try {
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
        } finally {
        	IOUtils.closeQuietly(pemReader);
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
		
        PEMReader pemReader = null;
        try {
        	pemReader = new PEMReader(new FileReader(fileName));
        	Object pemObject = null;

        	while ((pemObject = pemReader.readObject()) != null) {
        		if (pemObject instanceof X509Certificate) {
        			certificate = (X509Certificate) pemObject;
        			break;
        		}
        	}
        } finally {
        	IOUtils.closeQuietly(pemReader);
        }
        return certificate;
    }
    
    /**
     * Locate the default certificate.  The default location is constructed based
     * on user environment properties, so this method constructs a path and tests
     * whether or not the resulting path exists.
     * 
     * see also {@link http://www.cilogon.org/cert-howto#TOC-Finding-CILogon-Certificates}
     * 
     * @return File object of known-to-exist location
     * @throws FileNotFoundException  if no default certificate can be located
     */
    public File locateDefaultCertificate() throws FileNotFoundException {
    	StringBuffer location = new StringBuffer();
    	
    	// the tmp dir
    	String tmp = System.getProperty("tmpdir");
    	if (tmp == null) {
    		tmp = "/tmp";
    	}

    	// UID
    	String uid = null;
    	BufferedReader reader = null;
    	try {
    		// get the user id from *nix systems
    		Process process = Runtime.getRuntime().exec("id -u");
    		int ret = process.waitFor();
    		if (ret == 0) {
    			InputStream stream = process.getInputStream();
    			reader = new BufferedReader(new InputStreamReader(stream));
    			String result = reader.readLine();
    			// verify it is a number
    			int testUid = Integer.parseInt(result);
    			uid = String.valueOf(testUid);
    		}
    	} catch (Exception e) {
			log.warn("No UID found, using user.name");
		} finally {
			IOUtils.closeQuietly(reader);
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
		
    	return fileLocation;
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
