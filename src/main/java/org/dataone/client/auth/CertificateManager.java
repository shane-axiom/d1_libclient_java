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
import java.io.FileInputStream;
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
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;
import javax.servlet.http.HttpServletRequest;
import org.dataone.exceptions.MarshallingException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.bouncycastle.asn1.ASN1InputStream;
import org.bouncycastle.asn1.ASN1Primitive;
import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.DERUTF8String;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jce.provider.X509CertificateObject;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.types.v1.Person;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SubjectInfo;
import org.dataone.service.types.v1.util.ChecksumUtil;
import org.dataone.service.util.TypeMarshaller;
import org.dataone.exceptions.MarshallingException;

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
 * Finally, note that the TLS protocol used to establish SSL/TLS connections is 
 * configurable using the property 'tls.protocol.preferences'.  Success of TLS 
 * connections relies on alignment of the chosen TLS protocol with the Security 
 * providers determined by your runtime environment. As of May, 2015, te preference 
 * list is 'TLSv1.2, TLS'. This allows standard Java6 runtimes to operate at 
 * TLSv1.0, and Java7 and 8 runtimes to use the more secure TLSv1.2.
 * 
 * This class is a singleton, as in any given application there 
 * need only be one collection of certificates.  
 * @author Matt Jones, Ben Leinfelder
 */
public class CertificateManager extends Observable {
    
    static Log log = LogFactory.getLog(CertificateManager.class);
//	private static Log trustManLog = LogFactory.getLog(X509TrustManager.class);

    // this can be set by caller if the default discovery mechanism is not applicable
    private String certificateLocation = null;
    private String certificateMD5Checksum = "";
    
    // other variables
    private String keyStorePassword = null;
    private String keyStoreType = null;
    private String tlsVersion = null;

    // this is packaged with the library
    private static final String shippedCAcerts = "/org/dataone/client/auth/d1-trusted-certs.crt";
//    private static final char[] caTrustStorePass = "dataONE".toCharArray();
    private KeyStore d1TrustStore;
    
    protected static String defaultTlsPreferences = "TLSv1.2, TLS";
   
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

	    	certificateMD5Checksum = getChecksum(getCertificateFile());
    	} catch (FileNotFoundException e) {
    	    log.warn(e.getMessage(),e);
    	} catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    
    /*
     * A helper routine to calculate checksums of the certificate files.
     */
    private String getChecksum(File file) throws IOException, NoSuchAlgorithmException {
        String checksumValue = "";
        FileInputStream currentCertFileInputStream = null;
        try {
            File currentCertFile = getCertificateFile();
            currentCertFileInputStream = new FileInputStream(currentCertFile);
            checksumValue = ChecksumUtil.checksum(currentCertFileInputStream, "MD5").getValue();
        } catch (FileNotFoundException e) {
            log.debug(e.getMessage(),e);
        } finally {
            if (currentCertFileInputStream != null) {
                IOUtils.closeQuietly(currentCertFileInputStream);
            }
        } 
        return checksumValue;

    }

    /*
     * This is a thread-safe, lazy-loading design for initializing singletons.
     * all static initialization of this classes members are complete before 
     * releasing the object CertificateMAnagerSingleton.instance to the caller.
     * 
     * @link http://stackoverflow.com/questions/7048198/thread-safe-singletons-in-java
     */
    private static class CertificateManagerSingleton {
        
        public static final CertificateManager instance = new CertificateManager();
        
    }
    
    
    /**
     * Return the singleton instance of this CertificateManager, creating it if needed.
     * @return an instance of CertificateManager
     */
    public static CertificateManager getInstance() {
        return CertificateManagerSingleton.instance;
    }

    public String getCertificateLocation() {
        return certificateLocation;
    }

    /**
     * Use this method to set the certificate to point CertificateManager to
     * a certificate at the designated file-path.  (Call before getKeyStore()
     * or getSSLSocketFactory().
     * 
     * Triggers CertificateManager to call Observer.update() method on registered
     * observers unless the file is the same file as the previous set location 
     * (as determined by comparing the checksums of the current one with the new one).
     * 
     * @param certificateFilePath
     */
    public void setCertificateLocation(String certificateFilePath) {
        String oldLocation = this.certificateLocation;
        String oldChecksum = this.certificateMD5Checksum;
 
        this.certificateLocation = certificateFilePath;
        try {
            this.certificateMD5Checksum = getChecksum(getCertificateFile());
        } catch (IOException | NoSuchAlgorithmException e) {
            this.certificateMD5Checksum = "";
            log.debug(e.getMessage(),e);
        }
        
        log.debug(oldChecksum);
        log.debug(this.certificateMD5Checksum);
        if (oldChecksum.equals(this.certificateMD5Checksum)
                && Objects.equals(oldLocation, this.certificateLocation)) {
            // no notification, it's the same file
        } else {
            // different files
            setChanged();
            notifyObservers();
        }
    }


    /**
     * Registers the default certificate into the registry, using first the setLocation
     * or if null, the default CILogon location.
     * 
     * Supports the use of the default certificate in Session parameters of DataONE
     * API calls.
     * 
     * @throws IOException
     */
    public void registerDefaultCertificate() throws IOException {
        // TODO: characterize the types of exceptions returned, especially private key issues
        File certFile = getCertificateFile();
        X509Session session = this.getX509Session(certFile);
        String subjectDN = this.getSubjectDN(session.getCertificate());
        this.registerCertificate(subjectDN, session.getCertificate(), session.getPrivateKey());
    }
    
    
    /**
     * Register certificates to be used by getSSLSocketFactory(subject) for setting
     * up connections, using the subject as the lookup key
     * @param subjectString
     * @param certificate
     * @param key
     */
    public void registerCertificate(String subjectString, X509Certificate certificate, PrivateKey key) {
        if (log.isDebugEnabled())
            log.debug("registering certificate for: " + subjectString);

        certificates.put(subjectString, certificate);
        keys.put(subjectString, key);

        if (log.isDebugEnabled()) {
            Set<String> labels = certificates.keySet();
//            labels.addAll(keys.keySet());
            Iterator<String> it = labels.iterator();
            while (it.hasNext()) {
                String label = it.next();
                log.debug(String.format("   label:%s,  cert:%s, key:%s",
                        label, certificates.get(label).getSubjectX500Principal(), keys.get(label)));
            }
        }
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
                log.info("certificate.truststore.aux.location=" + auxLocation);
                
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
                    log.info("shippedCAcerts=" + shippedCAcerts);

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
                        log.debug("loadTrustStore: " + aliases.nextElement());
                    }
                    log.debug("loadTrustStore: " + this.d1TrustStore.aliases());
                }
            } catch (KeyStoreException e) {
                log.error(e.getMessage(), e);
//            } catch (FileNotFoundException e) {
//                log.error(e.getMessage(), e);
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
        PEMParser pemReader = null;
        try {
            pemReader = new PEMParser(certLoc);

            Object pemObject;
            log.info("loading into client truststore: " + certLoc);
            while ((pemObject = pemReader.readObject()) != null) {
                log.debug("pemObject: " + pemObject);
                if (pemObject instanceof X509CertificateHolder) {
                	X509CertificateHolder holder = (X509CertificateHolder) pemObject;
    				X509CertificateObject certificate = new X509CertificateObject(holder.toASN1Structure());

                    String alias = certificate.getSubjectX500Principal().getName();
                    log.debug("alias: " + alias);

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
        } catch (CertificateParsingException e) {
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
     * location, then using the default location. If a setCertificateLocation() has been
     * previously set and can't be found, a WARN is logged and null is returned.
     * 
     *
     * @return the loaded X.509 certificate or null if a certificate isn't found
     * 
     */
    public X509Certificate loadCertificate() {

        X509Certificate cert = null;
        try {
            // load up the PEM
            KeyStore keyStore = getKeyStore((String)null);
            if (keyStore != null) 
                cert = (X509Certificate) keyStore.getCertificate("cilogon");
        } catch (FileNotFoundException e) {
            log.warn(e.getMessage(),e);
        } catch (KeyStoreException | NoSuchAlgorithmException | 
                CertificateException | IOException e) {
            log.error(e.getMessage(),e);
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
            KeyStore keyStore = getKeyStore((String) null);
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
            ASN1Primitive derObject = toDERObject(extensionValue);
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
    private ASN1Primitive toDERObject(byte[] data) throws IOException {

        ASN1InputStream asnInputStream = null;
        ASN1Primitive dero = null;
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
     * @throws MarshallingException
     */
    public SubjectInfo getSubjectInfo(X509Certificate certificate)
    throws IOException, InstantiationException, IllegalAccessException, MarshallingException
    {
        String subjectInfoValue = this.getExtensionValue(certificate, CILOGON_OID_SUBJECT_INFO);
        if (log.isDebugEnabled())
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
        if (log.isDebugEnabled())
            log.debug("name: " + name);
        X500Principal principal = new X500Principal(name);
        String standardizedName = principal.getName(X500Principal.RFC2253);
        if (log.isDebugEnabled())
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
            
            // set the bare minimum if we have to
            if (subjectInfo == null) {
            	subjectInfo = new SubjectInfo();
            	Person person = new Person();
            	person.setSubject(subject);
            	person.setFamilyName("Unknown");
            	person.addGivenName("Unknown");
				subjectInfo.addPerson(person);
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
        if (log.isDebugEnabled())
            log.debug("javax.servlet.request.X509Certificate " + " = " + certificate);
        Object sslSession = request.getAttribute("javax.servlet.request.ssl_session");
        if (log.isDebugEnabled())
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
     * @throws NoSuchAlgorithmException - thrown if the default or configured TLS protocol
     *          is not supported by the java runtime.  To change the configured value to align
     *          with your runtime, see 'tls.protocol.preferences'  in auth.properties file.
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
        return getSSLSocketFactory(keyStore);
    }


    public SSLSocketFactory getSSLSocketFactory(X509Session x509Session)
    throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException,
    KeyManagementException, CertificateException, IOException
    {
        return getSSLSocketFactory(getKeyStore(x509Session));
    }

    private SSLSocketFactory getSSLSocketFactory(KeyStore keyStore)
    throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException,
    KeyManagementException
    {
        SSLSocketFactory socketFactory = null;

        // create SSL context
        SSLContext ctx = buildSSLContext(); 

        // based on config options, we get an appropriate truststore
        X509TrustManager tm = getTrustManager();

        // specify the client key manager
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());
        KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();

        // initialize the context
        ctx.init(keyManagers, new TrustManager[]{tm}, new SecureRandom());
        if (trustStoreIncludesD1CAs) {
            log.info("getSSLSocketFactory: using allow-all hostname verifier");
            //socketFactory = new SSLSocketFactory(ctx, SSLSocketFactory.STRICT_HOSTNAME_VERIFIER);
            //socketFactory = new SSLSocketFactory(ctx, SSLSocketFactory.BROWSER_COMPATIBLE_HOSTNAME_VERIFIER);
            socketFactory = new SSLSocketFactory(ctx, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
        } else {
            socketFactory = new SSLSocketFactory(ctx);
        }
        return socketFactory;
    }

    
    private SSLContext buildSSLContext() throws NoSuchAlgorithmException {
     
        SSLContext ctx = null;
        String tlsPreferences = Settings.getConfiguration().getString("tls.protocol.preferences",defaultTlsPreferences);
        String[] ctxPreferences = StringUtils.split(tlsPreferences,',');
        for (String preference : ctxPreferences) {
            try {
                log.info("...trying SSLContext protocol: " + preference);
                ctx = SSLContext.getInstance(StringUtils.trim(preference));
                log.info("...setting SSLContext with protocol: " + StringUtils.trim(preference));
                break;
            } catch (NoSuchAlgorithmException e) {
                ; // ok, try the next one
            }
        }
        if (ctx == null) {
            throw new NoSuchAlgorithmException("None of the preferred TLS protocols were found! (" +
                    tlsPreferences + ")");
        }
        return ctx;
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
     * @throws NoSuchAlgorithmException - thrown if the default or configured TLS protocol
     *          is not supported by the java runtime.  To change the configured value to align
     *          with your runtime, see 'tls.protocol.preferences'  in auth.properties file.
     * @throws UnrecoverableKeyException
     * @throws KeyStoreException - thrown if an unknown subject value provided
     * @throws KeyManagementException
     * @throws CertificateException
     * @throws IOException
     */
    public SSLConnectionSocketFactory getSSLConnectionSocketFactory(String subjectString)
    throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException,
    KeyManagementException, CertificateException, IOException
    {
        // our return object
        log.info("Entering getSSLConnectionSocketFactory");
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
        return getSSLConnectionSocketFactory(keyStore);
    }


    public SSLConnectionSocketFactory getSSLConnectionSocketFactory(X509Session x509Session)
    throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException,
    KeyManagementException, CertificateException, IOException
    {
        return getSSLConnectionSocketFactory(getKeyStore(x509Session));
    }


    private SSLConnectionSocketFactory getSSLConnectionSocketFactory(KeyStore keyStore)
    throws NoSuchAlgorithmException, KeyStoreException, UnrecoverableKeyException,
    KeyManagementException
    {

        SSLConnectionSocketFactory socketFactory = null;
        SSLContext ctx = buildSSLContext();
        
        // based on config options, we get an appropriate truststore
        X509TrustManager tm = getTrustManager();

        // specify the client key manager
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());
        KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();

        // initialize the context
        ctx.init(keyManagers, new TrustManager[]{tm}, new SecureRandom());
        if (trustStoreIncludesD1CAs) {
            log.info("getSSLConnectionSocketFactory: using allow-all hostname verifier");
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

        log.debug("***** JVM Default Trust Managers ******");
        for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {
            log.debug(trustManager);
            if (trustManager instanceof X509TrustManager) {
                jvmTrustManager = (X509TrustManager) trustManager;
                if (log.isDebugEnabled())
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
     * Select the X509Session using the provided subjectString to search among
     * the registered certificates. If the subjectString parameter is null, finds
     * the certificate first using the set certificate location, and then the
     * default location for CILogon certificate downloads (the user's tmp directory).
     * If no certificate file is found, null is returned, signifying that a public
     * connection (certificate-less) will be used.
     *
     * NOTE: this implementation uses Bouncy Castle security provider
     * @param subjectString
     * @return
     * @throws IOException - if there was trouble reading the PEM file.
     */
    public X509Session selectSession(String subjectString) throws IOException {

        X509Session x509Session = null;

        // if we have a session subject, find the registered certificate and key
        if (subjectString != null) {
            log.info("selectSession: looking up registered certificate for: " + subjectString);
            x509Session = X509Session.create(
                    certificates.get(subjectString),
                    keys.get(subjectString));
        }
        else {
            try {
                File certFile = getCertificateFile();
                x509Session = getX509Session(certFile);

            } catch (FileNotFoundException e) {
                // that's ok
                log.info("Did not find a certificate for the subject specified: " + subjectString);
            }
        }
        return x509Session;
    }
    
    private File getCertificateFile() throws FileNotFoundException {
        // if the location has been set, use it
        File certFile = null;
        if (certificateLocation == null) {
            log.info("selectSession: using the default certificate location");
            certFile = locateDefaultCertificate();
            log.debug("selectSession: certificate location = " + certFile);
        } else {
            log.info("selectSession: Using client certificate location: " + certificateLocation);
            certFile = new File(certificateLocation);
            if (!certFile.exists()) {
                throw new FileNotFoundException("No certificate located in expected set location: " + certificateLocation);
            }
        }
        return certFile;
    }



    /**
     * Creates a KeyStore using the certificate material determined by selectSession.
     * If for some reason the subjectString retrieves a certificate without an
     * associated private key, the exception message will contain the text:
     *  "java.security.KeyStoreException: Cannot store non-PrivateKeys".
     * @throws CertificateException
     * @throws NoSuchAlgorithmException
     * @throws IOException
     */
    private KeyStore getKeyStore(String subjectString) throws KeyStoreException,
    NoSuchAlgorithmException, CertificateException, IOException {

        X509Session s = selectSession(subjectString);  // can return null...
        return getKeyStore(s);
    }


    /*
     * converts an X509Session into a KeyStore
     * null value acceptable for x509Session parameter
     */
    private KeyStore getKeyStore(X509Session x509Session) throws KeyStoreException,
    NoSuchAlgorithmException, CertificateException, IOException {

        // a null keystore is valid input to the schema registry
        if (x509Session == null) return null;

        KeyStore keyStore  = KeyStore.getInstance(keyStoreType);
        keyStore.load(null, keyStorePassword.toCharArray());
        Certificate[] chain = new Certificate[] { x509Session.getCertificate() };

        if (log.isDebugEnabled()) {
            log.debug("getKeyStore: privateKey value: " + ObjectUtils.identityToString(x509Session.getPrivateKey()));
        }
        keyStore.setKeyEntry("cilogon", x509Session.getPrivateKey(),
                keyStorePassword.toCharArray(), chain);

        
        return keyStore;
    }

    /*
     * reads the PEM file from the file-system and extracts the certificate and
     * private key.  Supports KeyPair and PrivateKey PEM objects.
     */
    private X509Session getX509Session(File pemFile) throws
    FileNotFoundException, IOException {

        PrivateKey privateKey = null;
        X509Certificate certificate = null;
        PEMParser pemReader = null;
        try {
            pemReader = new PEMParser(new FileReader(pemFile));
            Object pemObject = null;
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
            while ((pemObject = pemReader.readObject()) != null) {
                if (pemObject instanceof PrivateKeyInfo) {
                    PrivateKeyInfo pki = (PrivateKeyInfo) pemObject;
                    privateKey = converter.getPrivateKey(pki);
                }
                else if (pemObject instanceof PEMKeyPair) {
                    PEMKeyPair pkp = (PEMKeyPair) pemObject;
                    privateKey = converter.getPrivateKey(pkp.getPrivateKeyInfo());
                } else if (pemObject instanceof X509CertificateHolder) {
                    X509CertificateHolder holder = (X509CertificateHolder) pemObject;
                    try {
                        certificate = new X509CertificateObject(holder.toASN1Structure());
                    } catch (CertificateParsingException e) {
                        log.warn("Could not parse x509 certificate", e);
                    }
                }
            }
        } finally {
            IOUtils.closeQuietly(pemReader);
        }
        return X509Session.create(certificate, privateKey);
    }

    /**
     * Load PrivateKey object from given file
     * @param fileName
     * @param password 
     * @return
     * @throws IOException
     */
    public PrivateKey loadPrivateKeyFromFile(String fileName, String password) throws IOException {

        Object pemObject = null;
        PrivateKey privateKey = null;

        // is there a password for the key?
        PEMParser pemReader = null;
        try {
            pemReader = new PEMParser(new FileReader(fileName));
            
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter();

            while ((pemObject = pemReader.readObject()) != null) {
                if (pemObject instanceof PrivateKeyInfo) {
                	PrivateKeyInfo pki = (PrivateKeyInfo) pemObject;
                    privateKey = converter.getPrivateKey(pki);
                    break;
                }
                else if (pemObject instanceof PEMKeyPair) {
                    PEMKeyPair pkp = (PEMKeyPair) pemObject;
                    privateKey = converter.getPrivateKey(pkp.getPrivateKeyInfo());
                    break;
                } else if (pemObject instanceof PEMEncryptedKeyPair) {
                    log.debug("Encrypted key - we will use provided password");
                    PEMDecryptorProvider decProv = new JcePEMDecryptorProviderBuilder().build(password.toCharArray());
                    KeyPair kp = converter.getKeyPair(((PEMEncryptedKeyPair) pemObject).decryptKeyPair(decProv));
                    privateKey = kp.getPrivate();
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

        PEMParser pemReader = null;
        try {
            pemReader = new PEMParser(new FileReader(fileName));
            Object pemObject = null;

            while ((pemObject = pemReader.readObject()) != null) {
            	if (pemObject instanceof X509CertificateHolder) {
                	X509CertificateHolder holder = (X509CertificateHolder) pemObject;
    				try {
						certificate = new X509CertificateObject(holder.toASN1Structure());
						break;
					} catch (CertificateParsingException e) {
						log.warn("could not parse certificate", e);
					}
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

        if (log.isDebugEnabled())
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
