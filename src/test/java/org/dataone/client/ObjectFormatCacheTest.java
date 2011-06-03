package org.dataone.client;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.dataone.client.ObjectFormatCache;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.types.ObjectFormatList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ObjectFormatCacheTest {
	
	private static String fmtidStr = "text/csv";
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

  @Test
  public void testHarnessCheck() {
      assertTrue(true);
  }
  
  /**
   * Test getting the entire object format list.  The default list has at least
   * 31 entries.
   */
  @Test
  public void testListFormats() {
  	
  	int formatsCount = 31;
  	ObjectFormatList objectFormatList = ObjectFormatCache.listFormats();
  	assertTrue(objectFormatList.getTotal() >= formatsCount);
  	
  }
  
  /**
   * Test getting a single object format from the registered list
   */
  @Test
  public void testGetFormatFromString() {
  	
  	String knownFormat = "text/plain";
    
  	try {
	    
			String result = 
				ObjectFormatCache.getFormat(knownFormat).getFmtid().getValue();
	  	System.out.println("Expected result: " + knownFormat);
	  	System.out.println("Found    result: " + result);
	  	assertTrue(result.equals(knownFormat));
  
    } catch (NullPointerException npe) {
	  
	    fail("The returned format was null: " + npe.getMessage());
    
    } catch (NotFound nfe) {
      
    	fail("The format " + knownFormat + " was not found.");
    	
    }
  	
  }
  
  /**
   * Test getting a non-existent object format, returning NotFound
   */
  @Test
  public void testObjectFormatNotFoundException() {
  
  	String badFormat = "text/bad-format";
  	
  	try {
  		
	    String result = 
	    	ObjectFormatCache.getFormat(badFormat).getFmtid().getValue();
      
  	} catch (Exception e) {
	    
  		assertTrue(e instanceof NotFound);
  	}
  	
  }
  
}