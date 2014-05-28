package org.dataone.client;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;

public class HttpUtils {

	
	/**
	 * Sets the CONNECTION_TIMEOUT and SO_TIMEOUT values for the underlying httpClient.
	 * (max delay in initial response, max delay between tcp packets, respectively).  
	 * Uses the same value for both.
	 * 
	 * @param restClient - the MultipartRestClient implementation
	 * @param milliseconds
	 */
	public static void setTimeouts(MultipartRestClient rc, int milliseconds) 
	{
        Integer timeout = new Integer(milliseconds);
        
        if (rc instanceof D1RestClient) {
        	HttpClient hc = ((D1RestClient) rc).getHttpClient();
        
        	HttpParams params = hc.getParams();
        	// the timeout in milliseconds until a connection is established.
        	HttpConnectionParams.setConnectionTimeout(params, timeout);
        
        	//defines the socket timeout (SO_TIMEOUT) in milliseconds, which is the timeout
        	// for waiting for data or, put differently, a maximum period inactivity between
        	// two consecutive data packets).
        	HttpConnectionParams.setSoTimeout(params, timeout);
      
        	((DefaultHttpClient)rc).setParams(params);
        }
	}
    
    
}
