package org.dataone.client.v1.examples;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.dataone.client.auth.CertificateManager;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.rest.DefaultHttpMultipartRestClient;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.service.exceptions.BaseException;

/**
 * A class to do a straightforward HTTP GET request and stream the response to stdout.
 * 
 * @author rnahf
 *
 */
public class SimpleHttpGetClient {



    /**
     * A simple command line interface that exercises the HttpClient and CertificateManager
     * to make a single HTTP/S GET call.
     * 
     * @param args: first argument is the url, the second is optional and is the filepath to the certificate
     * 
     * @throws ClientSideException 
     * @throws IOException 
     * @throws BaseException 
     * 
     */
    public static void main(String[] args) throws IOException, ClientSideException, BaseException {


        // parse arguments

        String url = null;
        if( args.length > 0 )
            url = args[0];

        String certificateLocation = null;
        if( args.length > 1) 
            certificateLocation = args[1];


        if (url == null) {
            System.err.println("the url is null");
            return;
        }


        if (certificateLocation != null)
            CertificateManager.getInstance().setCertificateLocation(certificateLocation);

        MultipartRestClient mrc = new DefaultHttpMultipartRestClient();

        InputStream is = mrc.doGetRequest(url, 10000);

        IOUtils.copy(is, System.out);

    }
}
