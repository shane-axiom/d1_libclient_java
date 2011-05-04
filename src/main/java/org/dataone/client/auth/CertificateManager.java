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

import javax.net.ssl.TrustManagerFactory;

/**
 * Import and manage certificates to be used for authentication against DataONE
 * service providers.  This class is a singleton, as in any given application there 
 * need only be one collection of certificates.  
 * @author Matt Jones
 */
public class CertificateManager {
    private static final String truststore = "/Users/jones/Desktop/cilogon/cilogon-trusted-certs";
    private static final String user_p12_pass = "certpwgoeshere";
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
            System.setProperty("javax.net.ssl.trustStore", truststore);
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
    
    /**
     * Find the CA certificate to be used to validate user certificates.
     * @return X509Certificate for the root CA
     */
    public X509Certificate getCACert(String caAlias) {
        X509Certificate caCert = null;
        TrustManagerFactory tmf;
        KeyStore trustStore = null;
        try {
            trustStore = KeyStore.getInstance("JKS");
            trustStore.load(new FileInputStream(System.getProperty("javax.net.ssl.trustStore")), null);
            caCert = (X509Certificate)trustStore.getCertificate(caAlias);
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
    
    public void loadPK12Certificate(String pk12_cert_store) {
        KeyStore ks;
        try {
            ks = KeyStore.getInstance("PKCS12");
            //ks.load(new FileInputStream(pk12_cert_store),"yourPassword".toCharArray());
            ks.load(new FileInputStream(pk12_cert_store), user_p12_pass.toCharArray());
            Enumeration<String> aliases = ks.aliases();
           
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                System.out.println("Alias: " + alias);
                if (ks.isCertificateEntry(alias)) {
                    Certificate cert = ks.getCertificate(alias);
                    System.out.println("Certificate Type: " + cert.getType());
                } else if (ks.isKeyEntry(alias)) {
                    System.out.println("This is a key!");
                    Key key = ks.getKey(alias, user_p12_pass.toCharArray());
                    System.out.println(key.getFormat());
                    Certificate cert[] = ks.getCertificateChain(alias);
                    X509Certificate x509cert = (X509Certificate) cert[0];
                    System.out.println("Certificate subject: " + x509cert.getSubjectDN().toString());
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
    }
    
    /**
     * Load a certificate from a file.
     * @param user_cert_name the name of the file containing the certificate.
     */
    public boolean verify(X509Certificate cert, X509Certificate caCert) {
        InputStream inStream;
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
