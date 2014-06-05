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

package org.dataone.client.impl.rest;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.impl.client.DefaultHttpClient;
import org.dataone.client.impl.rest.RestClient;
import org.dataone.mimemultipart.SimpleMultipartEntity;
import org.dataone.service.util.D1Url;
import org.junit.Test;


public class EchoTestRestClientIT {
	private static String echoNode = "http://dev-testing.dataone.org/testsvc";
	private static String echoResource = "echo";
	private static String mmEchoResource = "echomm";
	
	@Test
	public void testDoGetRequest() throws ClientProtocolException, IOException {
		D1Url u = new D1Url(echoNode, echoResource);
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		RestClient rc = new RestClient(new DefaultHttpClient());
		HttpResponse resp = rc.doGetRequest(u.getUrl(), null);
		InputStream is = resp.getEntity().getContent();
		String contentString = IOUtils.toString(is);
		System.out.println(contentString);
		assertTrue("",contentString.contains("request.META[ REQUEST_METHOD ] = GET"));		
		assertTrue("",contentString.contains("request.META[ PATH_INFO ] = /echo/bizz"));
		assertTrue("",contentString.contains("request.META[ QUERY_STRING ] = x=y"));
	}

	@Test
	public void testdoDeleteRequest() throws ClientProtocolException, IOException {
		D1Url u = new D1Url(echoNode, echoResource);
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		RestClient rc = new RestClient(new DefaultHttpClient());
		HttpResponse resp = rc.doDeleteRequest(u.getUrl(), null);
		InputStream is = resp.getEntity().getContent();
		String contentString = IOUtils.toString(is);
		System.out.println(contentString);
		assertTrue("",contentString.contains("request.META[ REQUEST_METHOD ] = DELETE"));		
		assertTrue("",contentString.contains("request.META[ PATH_INFO ] = /echo/bizz"));
		assertTrue("",contentString.contains("request.META[ QUERY_STRING ] = x=y"));
	}
	
	@Test
	public void testDoHeadRequest() throws ClientProtocolException, IOException {
		D1Url u = new D1Url(echoNode, echoResource);
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		RestClient rc = new RestClient(new DefaultHttpClient());
		HttpResponse resp = rc.doHeadRequest(u.getUrl(), null);
		Header[] headers = resp.getAllHeaders();
		String hString = new String();
		for (int j=0; j<headers.length; j++) {
			hString += headers[j].getName() + " : " + headers[j].getValue() + "\n";
		}
		assertTrue("",hString.contains("Content-Type : text/plain"));		
	}
	
	@Test
	public void testdoPutRequestNullBody() throws ClientProtocolException, IOException {
		D1Url u = new D1Url(echoNode, echoResource);
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		RestClient rc = new RestClient(new DefaultHttpClient());
		HttpResponse resp = rc.doPutRequest(u.getUrl(),null, null);
		InputStream is = resp.getEntity().getContent();
		String contentString = IOUtils.toString(is);
		System.out.println(contentString);
		assertTrue("",contentString.contains("request.META[ REQUEST_METHOD ] = PUT"));		
		assertTrue("",contentString.contains("request.META[ PATH_INFO ] = /echo/bizz"));
		assertTrue("",contentString.contains("request.META[ QUERY_STRING ] = x=y"));
	}
	
	@Test
	public void testdoPostRequestNullBody() throws ClientProtocolException, IOException {
		D1Url u = new D1Url(echoNode, echoResource);
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		RestClient rc = new RestClient(new DefaultHttpClient());
		HttpResponse resp = rc.doPostRequest(u.getUrl(),null, null);
		InputStream is = resp.getEntity().getContent();
		String contentString = IOUtils.toString(is);
		System.out.println(contentString);
		assertTrue("",contentString.contains("request.META[ REQUEST_METHOD ] = POST"));		
		assertTrue("",contentString.contains("request.META[ PATH_INFO ] = /echo/bizz"));
		assertTrue("",contentString.contains("request.META[ QUERY_STRING ] = x=y"));
	}
	
//	@Test
	public void testdoPutRequest() throws ClientProtocolException, IOException {
		D1Url u = new D1Url(echoNode, mmEchoResource);
		u.addNextPathElement("bizz");
//		u.addNonEmptyParamPair("x", "y");
		SimpleMultipartEntity ent = new SimpleMultipartEntity();
		ent.addParamPart("Jabberwocky", "Twas brillig and the slithy tove, did gyre and gimble in the wabe");
		ent.addFilePart("Jabberwocky2", "All mimsy was the borogrove, and the mome wrath ungrabe.");
		RestClient rc = new RestClient(new DefaultHttpClient());
		HttpResponse resp = rc.doPutRequest(u.getUrl(),ent, null);
		InputStream is = resp.getEntity().getContent();
		String contentString = IOUtils.toString(is);
		System.out.println(contentString);
		assertTrue("",contentString.contains("request.META[ CONTENT_TYPE ] = multipart/form-data"));		
		assertTrue("",contentString.contains("request.REQUEST[ Jabberwocky ] = Twas brillig and the slithy tove, did gyre and gimble in the wabe"));
		assertTrue("",contentString.contains("request.FILES=<MultiValueDict: {u'Jabberwocky2': [<InMemoryUploadedFile: mmp.output."));
	}
	
	@Test
	public void testdoPostRequest() throws ClientProtocolException, IOException {
		D1Url u = new D1Url(echoNode, mmEchoResource);
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		SimpleMultipartEntity ent = new SimpleMultipartEntity();
		ent.addParamPart("Jabberwocky", "Twas brillig and the slithy tove, did gyre and gimble in the wabe");
		ent.addFilePart("Jabberwocky2", "All mimsy was the borogrove, and the mome wrath ungrabe.");
		RestClient rc = new RestClient(new DefaultHttpClient());
		HttpResponse resp = rc.doPostRequest(u.getUrl(),ent, null);
		InputStream is = resp.getEntity().getContent();
		String contentString = IOUtils.toString(is);
		System.out.println(contentString);
		assertTrue("",contentString.contains("request.META[ CONTENT_TYPE ] = multipart/form-data"));		
		assertTrue("",contentString.contains("request.REQUEST[ Jabberwocky ] = Twas brillig and the slithy tove, did gyre and gimble in the wabe"));
		assertTrue("",contentString.contains("request.FILES=<MultiValueDict: {u'Jabberwocky2': [<InMemoryUploadedFile: mmp.output"));
	}
	
	
	@Test
	public void testDoGetRequest_setHeader() throws ClientProtocolException, IOException {
		D1Url u = new D1Url(echoNode, echoResource);
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		RestClient rc = new RestClient(new DefaultHttpClient());
		rc.setHeader("mememe", "momomo");
		HttpResponse resp = rc.doGetRequest(u.getUrl(), null);
		InputStream is = resp.getEntity().getContent();
		String contentString = IOUtils.toString(is);
		System.out.println(contentString);
		assertTrue("",contentString.contains("request.META[ REQUEST_METHOD ] = GET"));		
		assertTrue("",contentString.contains("request.META[ PATH_INFO ] = /echo/bizz"));
		assertTrue("",contentString.contains("request.META[ QUERY_STRING ] = x=y"));
		assertTrue("",contentString.contains("request.META[ HTTP_MEMEME ] = momomo"));
	}
}
