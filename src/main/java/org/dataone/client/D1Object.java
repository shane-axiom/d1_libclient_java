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
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.AuthToken;
import org.dataone.service.types.Identifier;
import org.dataone.service.types.ObjectFormat;
import org.dataone.service.types.ObjectLocation;
import org.dataone.service.types.ObjectLocationList;
import org.dataone.service.types.SystemMetadata;

/**
 * A representation of an object that can be stored in the DataONE system. Objects
 * include both data objects and science metadata objects, and are differentiated
 * based on their ObjectFormat as stored in SystemMetadata.
 */
public class D1Object {

    private SystemMetadata sysmeta;
    // TODO: this should also be able to be a reference to data, rather than a value, with late binding to allow efficient implementations
    private byte[] data;
    
    /**
     * Create an object that contains the system metadata and data bytes from the D1 system.
     * @param id the Identifier to use to get objects from D1
     */
    public D1Object(Identifier id) {
        download(id);
    }

    /**
     * @return the identifier
     */
    public Identifier getIdentifier() {
        return sysmeta.getIdentifier();
    }
    
    /**
     * @return the type
     */
    public ObjectFormat getType() {
        return sysmeta.getObjectFormat();
    }

    /**
     * @return the sysmeta
     */
    public SystemMetadata getSystemMetadata() {
        return sysmeta;
    }

    /**
     * @param sysmeta the sysmeta to set
     */
    public void setSystemMetadata(SystemMetadata sysmeta) {
        assert(sysmeta != null);
        this.sysmeta = sysmeta;
    }

    /**
     * @return the data
     */
    public byte[] getData() {
        return data;
    }

    /**
     * @param data the data to set
     */
    public void setData(byte[] data) {
        assert(data != null);
        this.data = data;
    }

    public List<Identifier> getDescribeList() {
        return sysmeta.getDescribeList();
    }
    
    public List<Identifier> getDescribeByList() {
        return sysmeta.getDescribedByList();
    }

    public List<Identifier> getObsoletedByList() {
        return sysmeta.getObsoletedByList();
    }
    
    /**
     * Contact D1 services to download the metadata and data.
     * @param id identifier to be downloaded
     */
    private void download(Identifier id) {
        D1Object o = null;
    
        CNode cn = D1Client.getCN();
        AuthToken token = new AuthToken();
    
        ObjectLocationList oll;
        try {
            // Get the system metadata for the object
            SystemMetadata m = cn.getSystemMetadata(token, id);
            if (m != null) {
                setSystemMetadata(m);
            }
            
            // Resolve the MNs that contain the object
            oll = cn.resolve(token, id);
            // Try each of the locations until we find the object
            for (ObjectLocation ol : oll.getObjectLocationList()) {
                System.out.println("   === Trying Location: "
                        + ol.getNodeIdentifier().getValue() + " ("
                        + ol.getUrl() + ")");
                               
                // Get the contents of the object itself
                MNode mn = D1Client.getMN(ol.getBaseURL());
                InputStream is = mn.get(token, id);
                try {
                    setData(IOUtils.toByteArray(is));
                } catch (IOException e) {
                    // Couldn't get the object from this object location
                    // So move on to the next
                    e.printStackTrace();
                }
            }
        } catch (InvalidToken e) {
            e.printStackTrace();
        } catch (ServiceFailure e) {
            e.printStackTrace();
        } catch (NotAuthorized e) {
            e.printStackTrace();
        } catch (NotFound e) {
            e.printStackTrace();
        } catch (InvalidRequest e) {
            e.printStackTrace();
        } catch (NotImplemented e) {
            e.printStackTrace();
        }
    }
}
