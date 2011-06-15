package org.dataone.client.auth;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Principal;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.servlet.http.HttpServletRequest;

import org.apache.http.conn.ssl.SSLSocketFactory;
import org.dataone.service.types.Session;
import org.dataone.service.types.Subject;

/**
 * Import and manage certificates to be used for authentication against DataONE
 * service providers.  This class is a singleton, as in any given application there 
 * need only be one collection of certificates.  
 * @author Matt Jones
 */
public class CertificateManager {
	
	// these should be set by the caller
	private String keyStoreName = "/tmp/x509up_u503.p12";
	private String keyStorePassword = "changeitchangeit";
	
    // this is packaged with the library
    private static final String caTrustStore = "cilogon-trusted-certs";
    private static final String caTrustStorePass = "cilogon";

    private static CertificateManager cm = null;
    private CertificateFactory cf = null;
    
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
            cf = CertificateFactory.getInstance("X.509");
        } catch (CertificateException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
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
    
    public String getKeyStoreName() {
		return keyStoreName;
	}

	public void setKeyStoreName(String keyStoreName) {
		this.keyStoreName = keyStoreName;
	}

	public String getKeyStorePassword() {
		return keyStorePassword;
	}

	public void setKeyStorePassword(String keyStorePassword) {
		this.keyStorePassword = keyStorePassword;
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
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (CertificateException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return caCert;
    }
    
    /**
     * Load a certificate from a file.
     * @param user_cert_name the name of the file containing the certificate.
     */
    public X509Certificate loadCertificate(String user_cert_name) {
        InputStream inStream;
        X509Certificate cert = null;
        try {
            inStream = new FileInputStream(user_cert_name);
            cert = (X509Certificate) cf.generateCertificate(inStream);
            inStream.close();
        } catch (CertificateException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return cert;
    }
    
    public X509Certificate loadPK12Certificate() {
    	X509Certificate x509cert = null;
        KeyStore ks;
        try {
            ks = KeyStore.getInstance("PKCS12");
            ks.load(new FileInputStream(keyStoreName), keyStorePassword.toCharArray());
            Enumeration<String> aliases = ks.aliases();
           
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                System.out.println("Alias: " + alias);
                if (ks.isCertificateEntry(alias)) {
                    Certificate cert = ks.getCertificate(alias);
                    System.out.println("Certificate Type: " + cert.getType());
                } else if (ks.isKeyEntry(alias)) {
                    System.out.println("This is a key!");
                    Key key = ks.getKey(alias, keyStorePassword.toCharArray());
                    System.out.println(key.getFormat());
                    Certificate cert[] = ks.getCertificateChain(alias);
                    x509cert = (X509Certificate) cert[0];
                    System.out.println("Certificate subject: " + x509cert.getSubjectDN().toString());
                    break;
                }
            }
        } catch (KeyStoreException e) {
            // TODO Auto-generated catch block
            System.out.println("KSE: " + e.getMessage());
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (CertificateException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (UnrecoverableKeyException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return x509cert;
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
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            System.out.println("Certificate verification failed, invalid key.");
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            System.out.println("Certificate verification failed, no such algorithm.");
            e.printStackTrace();
        } catch (NoSuchProviderException e) {
            System.out.println("Certificate verification failed, no such provider.");
            e.printStackTrace();
        } catch (SignatureException e) {
            System.out.println("Certificate verification failed, signatures do not match.");
        }
        return isValid;
    }
    
    public Session getSession(HttpServletRequest request) {
    	Session session = new Session();
    	Object certificate = request.getAttribute("javax.servlet.request.X509Certificate");
    	System.out.println("javax.servlet.request.X509Certificate " + " = " + certificate);
    	Object sslSession = request.getAttribute("javax.servlet.request.ssl_session");
    	System.out.println("javax.servlet.request.ssl_session " + " = " + sslSession);
    	if (certificate instanceof X509Certificate[]) {
    		X509Certificate[] x509Certificates = (X509Certificate[]) certificate;
    		for (X509Certificate x509Cert:x509Certificates) {
	    		displayCertificate(x509Cert);
	    		Principal subjectDN = x509Cert.getSubjectDN();
	    		Subject subject = new Subject();
	    		subject.setValue(subjectDN.toString());
	    		session.setSubject(subject);
	    		//TODO get the SubjectList info
	    		break;
    		}
    	}
    	return session;
    }
    
    public SSLSocketFactory getSSLSocketFactory() throws Exception{
    	// our return object
    	SSLSocketFactory socketFactory = null;
    	
    	// get the keystore that will provide the material
    	KeyStore keyStore = null;
        FileInputStream instream = null;
        try {
        	keyStore  = KeyStore.getInstance("PKCS12");
            instream = new FileInputStream(keyStoreName);
            keyStore.load(instream, keyStorePassword.toCharArray());
        } 
        catch (Exception e) {
        	e.printStackTrace();
        }
        finally {
            try { 
            	instream.close();
            } 
            catch (Exception ignore) {
            	ignore.printStackTrace();
            }
        }
        
        // context-based?
        boolean useContext = true;
        if (useContext) {
            SSLContext ctx = SSLContext.getInstance("TLS");
            
            // use a very liberal trust manager for trusting the server
            X509TrustManager tm = new X509TrustManager() {
	            public void checkClientTrusted(X509Certificate[] xcs, String string) throws CertificateException {
	            	System.out.println("checkClientTrusted - " + string);
	            }
	            public void checkServerTrusted(X509Certificate[] xcs, String string) throws CertificateException {
	            	System.out.println("checkServerTrusted - " + string);
	            }
	            public X509Certificate[] getAcceptedIssuers() {
	            	System.out.println("getAcceptedIssuers");
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
        } else {
            socketFactory = new SSLSocketFactory(keyStore, keyStorePassword, keyStore);
        }
        socketFactory.setHostnameVerifier(SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
        
        return socketFactory;
    }
    
    
    /**
     * Show details of an X509 certificate, printing the information to STDOUT.
     * @param cert the certificate to be displayed
     */
    protected void displayCertificate(X509Certificate cert) {
        if (cert == null) {
            return;
        }
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        Principal issuerDN = cert.getIssuerDN();
        System.out.println(" Issuer: " + issuerDN.toString());
        Date notBefore = cert.getNotBefore();
        DateFormat fmt = SimpleDateFormat.getDateTimeInstance();
        System.out.println("   From: " + fmt.format(notBefore));
        Date notAfter = cert.getNotAfter();
        System.out.println("     To: " + fmt.format(notAfter));
        Principal subjectDN = cert.getSubjectDN();
        System.out.println("Subject: " + subjectDN.toString());
        System.out.println();
    }
}
