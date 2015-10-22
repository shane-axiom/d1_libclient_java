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

package org.dataone.client.auth;

import org.dataone.client.rest.HttpMultipartRestClient;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.service.types.v1.Session;


/**
 * A subclass of Session to hide the complexities of AuthTokens and connection
 * management from users.  It extends the existing Session datatype to incorporate
 * other client-owned constructs used to interact with dataone services, specifically
 * the MultipartRestClient and the authToken string.
 * 
 * The authToken property determines the Session's identity with servers,
 * so construction is determined from it and its associated information.
 * Other expensive-to-build objects are properties that can be set after 
 * instance construction.
 * 
 * @author rnahf
 *
 */
public class AuthTokenSession extends Session {

    private String authToken;
    private MultipartRestClient mrc;
    private Object httpClient;
    
    public AuthTokenSession(String authToken) {
        this.authToken = authToken;
        HttpMultipartRestClient hmrc = new HttpMultipartRestClient(this);
        this.setMultipartRestClient(hmrc);
        this.setHttpClient(hmrc.getHttpClient());
    }

    public String getAuthToken() {
        return this.authToken;
    }
    
    public void setHttpClient(Object httpClient) {
        this.httpClient = httpClient;
    }
    
    public Object getHttpClient() {
        return this.httpClient;
    }
    
    public void setMultipartRestClient(MultipartRestClient multipartRestClient) {
        this.mrc = multipartRestClient;
    }
    
    public MultipartRestClient getMultipartRestClient() {
        return this.mrc;
    }
}
