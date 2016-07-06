package org.dataone.client.auth;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.PrivateKey;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.Date;

import org.dataone.exceptions.MarshallingException;

import org.dataone.client.rest.MultipartRestClient;
import org.dataone.service.types.v1.Session;
import org.dataone.service.types.v2.TypeFactory;
import org.dataone.exceptions.MarshallingException;

/**
 * A subclass of Session to hide the complexities of certificate and connection
 * management from users.  It extends the existing Session datatype to incorporate
 * other client-owned constructs used to interact with dataone services, specifically
 * the MultipartRestClient and the X509Certificate.
 * 
 * The certificate is the unique part of the X509Session that determines the Session
 * attributes, so construction is determined from it and its associated information.
 * Other expensive-to-build objects are properties that can be set after 
 * instance construction.
 * 
 *
 *
 * @author rnahf
 *
 */
public class X509Session extends Session {

    private static final long serialVersionUID = -1711723814675675414L;
    
    private X509Certificate cert;
    private PrivateKey privateKey;
    private Object httpClient;
    private MultipartRestClient mrc;

    
    X509Session(X509Certificate certificate, PrivateKey key) {
        this.cert = certificate;
        this.privateKey = key;
        
    }

    private X509Session() {
        // TODO Auto-generated constructor stub
    }
    
    public static X509Session create(String certificateLabel) throws IOException {

        return CertificateManager.getInstance().selectSession(certificateLabel);
    }

    public static X509Session create(X509Certificate certificate, PrivateKey key, 
            Session session) throws InstantiationException, IllegalAccessException, 
            InvocationTargetException, MarshallingException, IOException, NoSuchMethodException {
        
        X509Session xs = TypeFactory.convertTypeFromType(TypeFactory.clone(session), X509Session.class);
        xs.cert = certificate;
        xs.privateKey = key;
        return xs;
    }
    
    public static X509Session create(X509Certificate certificate, PrivateKey key) {
        return new X509Session(certificate, key);
    }
    
    public void checkValidity() throws CertificateExpiredException, CertificateNotYetValidException {
        this.cert.checkValidity();
    }
    
    public void checkValidity(Date date) throws CertificateExpiredException, CertificateNotYetValidException {
        this.cert.checkValidity(date);
    }
    
    public X509Certificate getCertificate() {
        return this.cert;
    }
    
    public PrivateKey getPrivateKey() {
        return this.privateKey;
    }
    
    public void setHttpClient(Object httpClient) {
        this.httpClient = httpClient;
    }
    
    public Object getHttpClient() {
        return this.httpClient;
    }
    
    public void setMultipartRestClient(MultipartRestClient multipartRestClient) {
        this.mrc = multipartRestClient;
    }
    
    public MultipartRestClient getMultipartRestClient() {
        return this.mrc;
    }
    

}
