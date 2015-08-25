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

package org.dataone.client.v1.types;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;

import org.dataone.client.v2.formats.ObjectFormatCache;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.ObjectFormat;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.Subject;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.types.v1.TypeFactory;

public class D1TypeBuilder extends org.dataone.service.types.v1.TypeFactory {



	
    /**
     * Builds a minimal and 'typical' SystemMetadata object containing all of the required fields needed
     * for submission to DataONE at time of create.  'Typical' in this case denotes
     * that the rightsHolder and submitter are the same Subject. The Checksum and 
     * content length are derived from the InputStream.  
     * @param id
     * @param data
     * @param formatId
     * @param rightsHolder
     * @return
     * @throws NoSuchAlgorithmException
     * @throws IOException
     * @throws NotFound
     * @throws ServiceFailure
     */
    public static SystemMetadata buildMinimalSystemMetadata(Identifier id, InputStream data, 
            ObjectFormatIdentifier formatId, Subject rightsHolder) 
                    throws NoSuchAlgorithmException, IOException, NotFound, ServiceFailure {
        ObjectFormat fmt;
        try {
            fmt = ObjectFormatCache.getInstance().getFormat(formatId);
        }
        catch (BaseException be) {
            formatId.setValue("application/octet-stream");
            fmt = ObjectFormatCache.getInstance().getFormat(formatId);
        }
        return TypeFactory.buildMinimalSystemMetadata(id, data, "MD5", fmt.getFormatId(), 
                rightsHolder);
    }
}
