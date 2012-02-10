package org.dataone.client;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.client.ClientProtocolException;
import org.dataone.mimemultipart.SimpleMultipartEntity;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.util.D1Url;
import org.junit.Ignore;
import org.junit.Test;


public class EchoTestD1RestClientIT {
	private static String echoNode = "http://dev-testing.dataone.org/testsvc";
	private static String echoResource = "echo";
	private static String mmEchoResource = "echomm";
	
	@Test
	public void testDoGetRequest() 
	throws ClientProtocolException, IOException, HttpException, BaseException
	{
		D1Url u = new D1Url(echoNode, echoResource);
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		D1RestClient rc = new D1RestClient();
		String contentString = null;
		try {
			InputStream is = rc.doGetRequest(u.getUrl());
			contentString = IOUtils.toString(is);
		} catch (ServiceFailure e) {	
			contentString = e.getDescription();
		}
		System.out.println(contentString);
		assertTrue("",contentString.contains("request.META[ REQUEST_METHOD ] = GET"));		
		assertTrue("",contentString.contains("request.META[ PATH_INFO ] = /echo/bizz"));
		assertTrue("",contentString.contains("request.META[ QUERY_STRING ] = x=y"));
	}

	@Test
	public void testdoDeleteRequest() 
	throws ClientProtocolException, IOException, HttpException, BaseException
	{
		D1Url u = new D1Url(echoNode, echoResource);
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		D1RestClient rc = new D1RestClient();
		String contentString = null;
		try {
			InputStream is = rc.doDeleteRequest(u.getUrl());
			contentString = IOUtils.toString(is);
		} catch (ServiceFailure e) {	
			contentString = e.getDescription();
		}
		System.out.println(contentString);
		assertTrue("",contentString.contains("request.META[ REQUEST_METHOD ] = DELETE"));		
		assertTrue("",contentString.contains("request.META[ PATH_INFO ] = /echo/bizz"));
		assertTrue("",contentString.contains("request.META[ QUERY_STRING ] = x=y"));
	}

	@Ignore("need to rewrite for returns of exceptions from echo service")
	@Test
	public void testDoHeadRequest() 
	throws ClientProtocolException, IOException, HttpException, BaseException
	{
		D1Url u = new D1Url(echoNode, echoResource);
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		D1RestClient rc = new D1RestClient();
		Header[] headers = null;
		try {
			headers = rc.doHeadRequest(u.getUrl());
		} catch (ServiceFailure e) {	
			String contentString = e.getDescription();
			System.out.println(contentString);
		}
		String hString = new String();
		for (int j=0; j<headers.length; j++) {
			hString += headers[j].getName() + " : " + headers[j].getValue() + "\n";
		}
		assertTrue("",hString.contains("Content-Type : text/plain"));		
	}
	
	@Test
	public void testdoPutRequestNullBody() 
	throws ClientProtocolException, IOException, HttpException, BaseException
	{
		D1Url u = new D1Url(echoNode, echoResource);
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		D1RestClient rc = new D1RestClient();
		String contentString = null;
		try {
			InputStream is = rc.doPutRequest(u.getUrl(),null);
			contentString = IOUtils.toString(is);
		} catch (ServiceFailure e) {	
			contentString = e.getDescription();
		}
		System.out.println(contentString);
		assertTrue("",contentString.contains("request.META[ REQUEST_METHOD ] = PUT"));		
		assertTrue("",contentString.contains("request.META[ PATH_INFO ] = /echo/bizz"));
		assertTrue("",contentString.contains("request.META[ QUERY_STRING ] = x=y"));
	}
	
	@Test
	public void testdoPostRequestNullBody() 
	throws ClientProtocolException, IOException, HttpException, BaseException
	{
		D1Url u = new D1Url(echoNode, echoResource);
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		D1RestClient rc = new D1RestClient();
		String contentString = null;
		try {
			InputStream is = rc.doPostRequest(u.getUrl(),null);
			contentString = IOUtils.toString(is);
		} catch (ServiceFailure e) {	
			contentString = e.getDescription();
		}
		System.out.println(contentString);
		assertTrue("",contentString.contains("request.META[ REQUEST_METHOD ] = POST"));		
		assertTrue("",contentString.contains("request.META[ PATH_INFO ] = /echo/bizz"));
		assertTrue("",contentString.contains("request.META[ QUERY_STRING ] = x=y"));
	}
	
	@Ignore("Django doesn't handle PUT requests with mime-multipart")
	@Test
	public void testdoPutRequest() 
	throws ClientProtocolException, IOException, HttpException, BaseException
	{
		D1Url u = new D1Url(echoNode, mmEchoResource);
		u.addNextPathElement("bizz");
//		u.addNonEmptyParamPair("x", "y");
		SimpleMultipartEntity ent = new SimpleMultipartEntity();
		ent.addParamPart("Jabberwocky", "Twas brillig and the slithy tove, did gyre and gimble in the wabe");
		ent.addFilePart("Jabberwocky2", "All mimsy was the borogrove, and the mome wrath ungrabe.");
		D1RestClient rc = new D1RestClient();
		String contentString = null;
		try {
			InputStream is = rc.doPutRequest(u.getUrl(),ent);
			contentString = IOUtils.toString(is);
		} catch (ServiceFailure e) {	
			contentString = e.getDescription();
		}
		System.out.println(contentString);
		assertTrue("",contentString.contains("request.META[ CONTENT_TYPE ] = multipart/form-data"));		
		assertTrue("",contentString.contains("request.REQUEST[ Jabberwocky ] = Twas brillig and the slithy tove, did gyre and gimble in the wabe"));
		assertTrue("",contentString.contains("request.FILES=<MultiValueDict: {u'Jabberwocky2': [<InMemoryUploadedFile: mmp.output."));
	}
	
	@Test
	public void testdoPostRequest() 
	throws ClientProtocolException, IOException, HttpException, BaseException
	{
		D1Url u = new D1Url(echoNode, mmEchoResource);
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		SimpleMultipartEntity ent = new SimpleMultipartEntity();
		ent.addParamPart("Jabberwocky", "Twas brillig and the slithy tove, did gyre and gimble in the wabe");
		ent.addFilePart("Jabberwocky2", "All mimsy was the borogrove, and the mome wrath ungrabe.");
		D1RestClient rc = new D1RestClient();
		String contentString = null;
		try {
			InputStream is = rc.doPostRequest(u.getUrl(),ent);
			contentString = IOUtils.toString(is);
		} catch (ServiceFailure e) {	
			contentString = e.getDescription();
		}
		System.out.println(contentString);
		assertTrue("",contentString.contains("request.META[ CONTENT_TYPE ] = multipart/form-data"));		
		assertTrue("",contentString.contains("request.REQUEST[ Jabberwocky ] = Twas brillig and the slithy tove, did gyre and gimble in the wabe"));
		assertTrue("",contentString.contains("request.FILES=<MultiValueDict: {u'Jabberwocky2': [<InMemoryUploadedFile: mmp.output"));
	}
	
	@Test
	public void testExceptionFiltering() 
	throws ClientProtocolException, IOException, IllegalStateException, HttpException 
	{
		D1Url u = new D1Url(echoNode + "xx", "fakeResource");
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		D1RestClient rc = new D1RestClient();

		try {
			InputStream is = rc.doGetRequest(u.getUrl());
			String contentString = IOUtils.toString(is);
			System.out.println(contentString);
			fail("should not have reached here");
		} catch (NotFound e) {
			System.out.println("Exception thrown = " + e.getClass());
			System.out.println("Description = " + e.getDescription());
			assertTrue("exception thrown",  e instanceof NotFound);
		} catch (BaseException e) {
			System.out.println("Exception thrown = " + e.getClass());
			System.out.println("Description = " + e.getDescription());
			assertTrue("exception thrown",  e instanceof ServiceFailure);
		} catch (HttpException e) {
			System.out.println("Exception thrown = " + e.getClass());
			System.out.println("Message = " + e.getMessage());
			assertTrue("http exception thrown:",  e.getMessage().startsWith("Not Found"));
		}
		
	}
	
}
