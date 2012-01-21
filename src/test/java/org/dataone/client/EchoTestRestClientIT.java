package org.dataone.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
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
		RestClient rc = new RestClient();
		HttpResponse resp = rc.doGetRequest(u.getUrl());
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
		RestClient rc = new RestClient();
		HttpResponse resp = rc.doDeleteRequest(u.getUrl());
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
		RestClient rc = new RestClient();
		HttpResponse resp = rc.doHeadRequest(u.getUrl());
		Header[] headers = resp.getAllHeaders();
		String hString = null;
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
		RestClient rc = new RestClient();
		HttpResponse resp = rc.doPutRequest(u.getUrl(),null);
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
		RestClient rc = new RestClient();
		HttpResponse resp = rc.doPostRequest(u.getUrl(),null);
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
		RestClient rc = new RestClient();
		HttpResponse resp = rc.doPutRequest(u.getUrl(),ent);
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
		RestClient rc = new RestClient();
		HttpResponse resp = rc.doPostRequest(u.getUrl(),ent);
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
		RestClient rc = new RestClient();
		rc.setHeader("mememe", "momomo");
		HttpResponse resp = rc.doGetRequest(u.getUrl());
		InputStream is = resp.getEntity().getContent();
		String contentString = IOUtils.toString(is);
		System.out.println(contentString);
		assertTrue("",contentString.contains("request.META[ REQUEST_METHOD ] = GET"));		
		assertTrue("",contentString.contains("request.META[ PATH_INFO ] = /echo/bizz"));
		assertTrue("",contentString.contains("request.META[ QUERY_STRING ] = x=y"));
		assertTrue("",contentString.contains("request.META[ HTTP_MEMEME ] = momomo"));
	}
}
