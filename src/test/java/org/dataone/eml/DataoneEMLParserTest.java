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
