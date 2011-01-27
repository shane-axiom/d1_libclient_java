package org.dataone.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.dataone.mimemultipart.SimpleMultipartEntity;
import org.dataone.service.D1Url;
import org.dataone.service.exceptions.AuthenticationTimeout;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.InvalidCredentials;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidSystemMetadata;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.exceptions.UnsupportedType;
import org.junit.Test;


public class D1RestClientEchoTest {
	private static String echoNode = "http://dev-testing.dataone.org/testsvc";
	private static String echoResource = "echo";
	private static String mmEchoResource = "echomm";
	
	@Test
	public void testDoGetRequest() throws ClientProtocolException, IOException, NotFound, InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, UnsupportedType, InsufficientResources, InvalidSystemMetadata, NotImplemented, InvalidCredentials, InvalidRequest, IllegalStateException, AuthenticationTimeout {
		D1Url u = new D1Url(echoNode, echoResource);
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		D1RestClient rc = new D1RestClient();
//		HttpResponse resp = rc.doGetRequest(u.getUrl());
		InputStream is = rc.doGetRequest(u.getUrl());
		String contentString = IOUtils.toString(is);
		System.out.println(contentString);
		assertTrue("",contentString.contains("request.META[ REQUEST_METHOD ] = GET"));		
		assertTrue("",contentString.contains("request.META[ PATH_INFO ] = /echo/bizz"));
		assertTrue("",contentString.contains("request.META[ QUERY_STRING ] = x=y"));
	}

	@Test
	public void testdoDeleteRequest() throws ClientProtocolException, IOException, NotFound, InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, UnsupportedType, InsufficientResources, InvalidSystemMetadata, NotImplemented, InvalidCredentials, InvalidRequest, IllegalStateException, AuthenticationTimeout {
		D1Url u = new D1Url(echoNode, echoResource);
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		D1RestClient rc = new D1RestClient();
//		HttpResponse resp = 
		InputStream is = rc.doDeleteRequest(u.getUrl());
		String contentString = IOUtils.toString(is);
		System.out.println(contentString);
		assertTrue("",contentString.contains("request.META[ REQUEST_METHOD ] = DELETE"));		
		assertTrue("",contentString.contains("request.META[ PATH_INFO ] = /echo/bizz"));
		assertTrue("",contentString.contains("request.META[ QUERY_STRING ] = x=y"));
	}
	
	@Test
	public void testDoHeadRequest() throws ClientProtocolException, IOException, NotFound, InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, UnsupportedType, InsufficientResources, InvalidSystemMetadata, NotImplemented, InvalidCredentials, InvalidRequest, IllegalStateException, AuthenticationTimeout {
		D1Url u = new D1Url(echoNode, echoResource);
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		D1RestClient rc = new D1RestClient();
//		HttpResponse resp = rc.doHeadRequest(u.getUrl());
		Header[] headers = rc.doHeadRequest(u.getUrl());
		String hString = null;
		for (int j=0; j<headers.length; j++) {
			hString += headers[j].getName() + " : " + headers[j].getValue() + "\n";
		}
		assertTrue("",hString.contains("Content-Type : text/plain"));		
	}
	
	@Test
	public void testdoPutRequestNullBody() throws ClientProtocolException, IOException, NotFound, InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, UnsupportedType, InsufficientResources, InvalidSystemMetadata, NotImplemented, InvalidCredentials, InvalidRequest, IllegalStateException, AuthenticationTimeout {
		D1Url u = new D1Url(echoNode, echoResource);
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		D1RestClient rc = new D1RestClient();
//		HttpResponse resp = rc.doPutRequest(u.getUrl(),null);
		InputStream is = rc.doPutRequest(u.getUrl(),null);
		String contentString = IOUtils.toString(is);
		System.out.println(contentString);
		assertTrue("",contentString.contains("request.META[ REQUEST_METHOD ] = PUT"));		
		assertTrue("",contentString.contains("request.META[ PATH_INFO ] = /echo/bizz"));
		assertTrue("",contentString.contains("request.META[ QUERY_STRING ] = x=y"));
	}
	
	@Test
	public void testdoPostRequestNullBody() throws ClientProtocolException, IOException, NotFound, InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, UnsupportedType, InsufficientResources, InvalidSystemMetadata, NotImplemented, InvalidCredentials, InvalidRequest, IllegalStateException, AuthenticationTimeout {
		D1Url u = new D1Url(echoNode, echoResource);
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		D1RestClient rc = new D1RestClient();
		InputStream is = rc.doPostRequest(u.getUrl(),null);
		String contentString = IOUtils.toString(is);
		System.out.println(contentString);
		assertTrue("",contentString.contains("request.META[ REQUEST_METHOD ] = POST"));		
		assertTrue("",contentString.contains("request.META[ PATH_INFO ] = /echo/bizz"));
		assertTrue("",contentString.contains("request.META[ QUERY_STRING ] = x=y"));
	}
	
	@Test
	public void testdoPutRequest() throws ClientProtocolException, IOException, NotFound, InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, UnsupportedType, InsufficientResources, InvalidSystemMetadata, NotImplemented, InvalidCredentials, InvalidRequest, IllegalStateException, AuthenticationTimeout {
		D1Url u = new D1Url(echoNode, mmEchoResource);
		u.addNextPathElement("bizz");
//		u.addNonEmptyParamPair("x", "y");
		SimpleMultipartEntity ent = new SimpleMultipartEntity();
		ent.addParamPart("Jabberwocky", "Twas brillig and the slithy tove, did gyre and gimble in the wabe");
		ent.addFilePart("Jabberwocky2", "All mimsy was the borogrove, and the mome wrath ungrabe.");
		D1RestClient rc = new D1RestClient();
		InputStream is = rc.doPutRequest(u.getUrl(),ent);
		String contentString = IOUtils.toString(is);
		System.out.println(contentString);
		assertTrue("",contentString.contains("request.META[ CONTENT_TYPE ] = multipart/form-data"));		
		assertTrue("",contentString.contains("request.REQUEST[ Jabberwocky ] = Twas brillig and the slithy tove, did gyre and gimble in the wabe"));
		assertTrue("",contentString.contains("request.FILES=<MultiValueDict: {u'Jabberwocky2': [<InMemoryUploadedFile: mmp.output."));
	}
	
	@Test
	public void testdoPostRequest() throws ClientProtocolException, IOException, NotFound, InvalidToken, ServiceFailure, NotAuthorized, IdentifierNotUnique, UnsupportedType, InsufficientResources, InvalidSystemMetadata, NotImplemented, InvalidCredentials, InvalidRequest, IllegalStateException, AuthenticationTimeout {
		D1Url u = new D1Url(echoNode, mmEchoResource);
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		SimpleMultipartEntity ent = new SimpleMultipartEntity();
		ent.addParamPart("Jabberwocky", "Twas brillig and the slithy tove, did gyre and gimble in the wabe");
		ent.addFilePart("Jabberwocky2", "All mimsy was the borogrove, and the mome wrath ungrabe.");
		D1RestClient rc = new D1RestClient();
//		HttpResponse resp = rc.doPostRequest(u.getUrl(),ent);
		InputStream is = rc.doPostRequest(u.getUrl(),ent);
		String contentString = IOUtils.toString(is);
		System.out.println(contentString);
		assertTrue("",contentString.contains("request.META[ CONTENT_TYPE ] = multipart/form-data"));		
		assertTrue("",contentString.contains("request.REQUEST[ Jabberwocky ] = Twas brillig and the slithy tove, did gyre and gimble in the wabe"));
		assertTrue("",contentString.contains("request.FILES=<MultiValueDict: {u'Jabberwocky2': [<InMemoryUploadedFile: mmp.output"));
	}
	
	@Test
	public void testExceptionFiltering() throws ClientProtocolException, IOException {
		D1Url u = new D1Url(echoNode + "xx", "fakeResource");
		u.addNextPathElement("bizz");
		u.addNonEmptyParamPair("x", "y");
		D1RestClient rc = new D1RestClient();
//		HttpResponse resp = rc.doGetRequest(u.getUrl());
//		int status = resp.getStatusLine().getStatusCode();
//		System.out.println("status code = " + status);
		
		try {
//			is = rc.filterErrors(resp);
			InputStream is = rc.doGetRequest(u.getUrl());
			String contentString = IOUtils.toString(is);
			System.out.println(contentString);
			fail("should not have reached here");
		} catch (BaseException e) {
			System.out.println("Exception thrown = " + e.getClass());
			System.out.println("Description = " + e.getDescription());
			assertTrue("exception thrown",  e instanceof NotFound);
		}
	}
	
}
