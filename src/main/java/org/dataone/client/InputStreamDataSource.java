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

package org.dataone.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.activation.DataSource;

/**
 * Encapsulate an InputStream within a DataSource interface so that it is 
 * accessible to MIME processors.
 * 
 * @author Matthew Jones
 */
public class InputStreamDataSource implements DataSource {
    private String name;
    private InputStream stream;
    private boolean readOnce;
    
    public InputStreamDataSource(String name, InputStream stream) {
        super();
        this.name = name;
        this.stream = stream;
        this.readOnce = false;
    }

    public String getContentType() {
        return "application/octet-stream";
    }

    public InputStream getInputStream() throws IOException {
        if (readOnce) {
            throw new IOException("Only call getInputStream() once.");
        }
        readOnce = true;
        
        return stream;
    }

    public String getName() {
        return this.name;
    }

    public OutputStream getOutputStream() throws IOException {
        throw new IOException("Can't get an OutputStream from an InputStreamDataSource.");
    }
}
