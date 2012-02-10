package org.dataone.eml;

import static org.junit.Assert.fail;

import java.util.Iterator;

import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.util.ObjectFormatServiceImpl;
import org.junit.Test;


public class DataoneEMLParserTest {
	
	@Test
	public void testGetSupportedFormatIdentifierIterator() 
	throws ServiceFailure, NotImplemented {
		Iterator<String> it = DataoneEMLParser.getInstance().getSupportedFormatIdentifierIterator();
		
		ObjectFormatIdentifier formatId = new ObjectFormatIdentifier();
		
		while (it.hasNext()) {
			String formatIdString = it.next();
			formatId.setValue(formatIdString);
			try {
				ObjectFormatServiceImpl.getInstance().getFormat(formatId);
			} catch (NotFound e) {
				fail("cannot find the formatId '" + formatIdString + "' in the objectFormatCache");

			}
		}
		
	}
	
	@Test
	public void validateText_CSVformatString() 
	throws ServiceFailure, NotImplemented {
		
		ObjectFormatIdentifier formatId = new ObjectFormatIdentifier();
		
			String formatIdString = "text/csv";
			formatId.setValue(formatIdString);
			try {
				ObjectFormatServiceImpl.getInstance().getFormat(formatId);
			} catch (NotFound e) {
				fail("cannot find the formatId '" + formatIdString + "' in the objectFormatCache");

			}
		
	}
	
	@Test
	public void validateText_PlainFormatString() 
	throws ServiceFailure, NotImplemented {
		
		ObjectFormatIdentifier formatId = new ObjectFormatIdentifier();
		
			String formatIdString = "text/plain";
			formatId.setValue(formatIdString);
			try {
				ObjectFormatServiceImpl.getInstance().getFormat(formatId);
			} catch (NotFound e) {
				fail("cannot find the formatId '" + formatIdString + "' in the objectFormatCache");

			}
		
	}
	
	
	@Test
	public void validateApplicationOctetFormatString() 
	throws ServiceFailure, NotImplemented {
		
		ObjectFormatIdentifier formatId = new ObjectFormatIdentifier();
		
			String formatIdString = "application/octet-stream";
			formatId.setValue(formatIdString);
			try {
				ObjectFormatServiceImpl.getInstance().getFormat(formatId);
			} catch (NotFound e) {
				fail("cannot find the formatId '" + formatIdString + "' in the objectFormatCache");

			}
		
	}
	

}
