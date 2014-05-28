package org.dataone.client;

import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;

public class DefaultD1RestClient extends D1RestClient {

	public DefaultD1RestClient() {
		super(new DefaultHttpClient());
		HttpUtils.setTimeouts(this, 30*1000);
	}

	
	
}
