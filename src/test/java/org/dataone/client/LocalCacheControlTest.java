package org.dataone.client;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.NullInputStream;
import org.dataone.client.cache.LocalCache;
import org.dataone.client.exception.NotCached;
import org.dataone.service.types.v1.Identifier;
import org.junit.Test;
/**
 * Tests that the caching control in D1Node (handleCaching) works as expected
 * @author rnahf
 *
 */
public class LocalCacheControlTest {

	// tests use NullInputStream which emulates large data streams by generating 0's
	// from read method until the number of bytes read exceed the size set in the constructor
	
	@Test
	public void testHandleCaching_withinSizeLimit() {
		// can instantiate either MNode or CNode, doesn't matter.
		long contentLength = 10000;
		D1Node node = new MNode("foo");
		try {
			Identifier pid = D1TypeBuilder.buildIdentifier("within");
			InputStream is = node.handleCaching(pid, new NullInputStream(contentLength));
			
			System.out.println(is.getClass().getCanonicalName());
			byte[] bytes = IOUtils.toByteArray(is);
			assertEquals("byte count should match input",contentLength,bytes.length);
			
			try {
				LocalCache.instance().getData(pid);
			} catch (NotCached nc) {
				fail("Object should be cached");
			}
		} catch (IOException e) {
			fail("Got IOException:" + e.getMessage());
		}
	}
	
	@Test
	public void testHandleCaching_beyondSizeLimit() {
		// can instantiate either MNode or CNode, doesn't matter.
		long contentLength = 30 * 1024 * 1024;  // 30Mb where size limit is 10Mb
		D1Node node = new MNode("foo");
		try {
			Identifier pid = D1TypeBuilder.buildIdentifier("beyond");
			InputStream is = node.handleCaching(pid, new NullInputStream(contentLength));
			System.out.println(is.getClass().getCanonicalName());
			byte[] bytes = IOUtils.toByteArray(is);
			assertEquals("byte count should match input",contentLength,bytes.length);
			try {
				LocalCache.instance().getData(pid);
				fail("Object should not be cached");
			} catch (NotCached nc) {
				; // expect exception
			}
			
		} catch (IOException e) {
			fail("Got IOException:" + e.getMessage());
		}
	}

}
