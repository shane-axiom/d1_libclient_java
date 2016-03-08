package org.dataone.client.rest;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Observable;
import java.util.Observer;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.dataone.client.auth.CertificateManager;
import org.dataone.client.auth.X509Session;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.utils.HttpUtils;
import org.jibx.runtime.JiBXException;

/**
 * This subclass of HttpMultiparRestClient registers itself as an Observer of
 * CertificateManager, and updates its Restclient's HttpClient with a new one
 * using the CertificateManager's default certificate logic.
 * <p>
 * Update is triggered by the CertificateManager.setCertificateLocation method,
 * so that this MultipartRestClient is always in sync with CertificateManager.
 * <p> 
 * @author rnahf
 *
 */
public class DefaultHttpMultipartRestClient extends HttpMultipartRestClient implements Observer {

    /**
     * This MultipartRestClient implementation only uses the default or the set 
     * certificate location from CertificateManager to derive the Session.  It
     * also Observes the CertificateManager to update the HttpClient if default
     * certificate information changes.
     * 
     * @throws IOException
     * @throws ClientSideException
     */
    public DefaultHttpMultipartRestClient() throws IOException, ClientSideException {
        super((String)null);
        observeCertificateManager();
    }

    protected void observeCertificateManager() {
        CertificateManager.getInstance().addObserver(this);
    }

    /**
     * Updates the RestClient with new Session and certificate information from 
     * the CertificateManager.  Problems with replacing the HttpClient are logged
     * as errors, and 
     */
    @Override
    public void update(Observable observable, Object arg) {
        
        if (observable instanceof CertificateManager) {
            try {
                // using null for the selectSession parameter ensures we use the 
                // new default session configured into CertificateManager.
                this.x509Session = ((CertificateManager)observable).selectSession((String)null);
                HttpClient hc = this.rc.getHttpClient();
                this.rc.setHttpClient(HttpUtils.createHttpClient(x509Session));
//                if (hc instanceof CloseableHttpClient) {
//                    // TODO: is this ok to do?  could it cause interruption of stream reading?
//                    ((CloseableHttpClient)hc).close();
//                }
            } catch (UnrecoverableKeyException | KeyManagementException
                    | NoSuchAlgorithmException | KeyStoreException
                    | CertificateException | InstantiationException
                    | IllegalAccessException | JiBXException | IOException e) {

                log.error("Could not update the HttpClient with new default information from CertificateManager!", e);
            }
        } else {
            log.debug("Observer.update did not receive an Observable of the right type! Got a: " + observable.getClass().getCanonicalName());
        }
        
    }

}
