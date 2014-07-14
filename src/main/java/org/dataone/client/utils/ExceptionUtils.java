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
 */
package org.dataone.client.utils;

import java.util.Iterator;

import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.ServiceFailure;

public class ExceptionUtils {

	   /**
     * A helper function to preserve the stackTrace when catching one error and throwing a new one.
     * Also has some descriptive text which makes it clientSide specific
     * @param e
     * @return
     */
    public static ServiceFailure recastClientSideExceptionToServiceFailure(Exception e) {
    	ServiceFailure sfe = new ServiceFailure("0 Client_Error", e.getClass() + ": "+ e.getMessage());
                sfe.initCause(e);
		sfe.setStackTrace(e.getStackTrace());
    	return sfe;
    }

    
    /**
     * A helper function for recasting DataONE exceptions to ServiceFailures while
     * preserving the detail code and TraceDetails.

     * @param be - BaseException subclass to be recast
     * @return ServiceFailure
     */
    public static ServiceFailure recastDataONEExceptionToServiceFailure(BaseException be) {	
    	ServiceFailure sfe = new ServiceFailure(be.getDetail_code(), 
    			"Recasted unexpected exception from the service - " + be.getClass() + ": "+ be.getMessage());
    	
    	Iterator<String> it = be.getTraceKeySet().iterator();
    	while (it.hasNext()) {
    		String key = it.next();
    		sfe.addTraceDetail(key, be.getTraceDetail(key));
    	}
    	return sfe;
    }
	
}
