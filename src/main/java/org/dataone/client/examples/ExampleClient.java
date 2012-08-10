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
package org.dataone.client.examples;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigInteger;

import org.dataone.client.CNode;
import org.dataone.client.D1Client;
import org.dataone.client.MNode;
import org.dataone.client.auth.ClientIdentityManager;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.types.v1.*;

/**
 * ExampleClient is a command-line class that can be run to illustrate usage patterns
 * of the d1_libclient_java library methods and services.  Examples do not illustrate
 * all possible services that are available in the DataONE API, but rather are
 * meant to illustrate usage patterns.
 */
public class ExampleClient {

    /**
     * Execute the examples.
     */
    public static void main(String[] args) {
        // By default, use development.
        String cnUrl = System.getProperty("CN_URL", "https://cn-dev.test.dataone.org");
        Settings.getConfiguration().setProperty("D1Client.CN_URL", cnUrl);
        
        String currentUrl = "https://demo1.test.dataone.org:443/knb/d1/mn";
        MNode mn = D1Client.getMN(currentUrl);

        runExampleCreate(mn);

        String currentMNodeId = "urn:node:mnDemo5";
        runExampleCreate_version2(currentMNodeId);
    }

    /**
     * Demonstrate the execution of the MNRead.create() service on the given
     * Member Node.  This method creates a data object on the node with an
     * identifier based on the current date in milliseconds, so it  
     * should only be run against test servers, not production servers, to
     * avoid polluting production servers with test data.
     * For simplicity, this example skips the best-practice of reserving the
     * identifier prior to creating the object. 
     * 
     * @param mn the MNode member node on which to create the object
     */
    private static void runExampleCreate(MNode mn) {
        try {
            Identifier newid = new Identifier();
            String idstr = "test:" + System.currentTimeMillis();
            newid.setValue(idstr);
            String csv = "1,2,3";
            InputStream is = new ByteArrayInputStream(csv.getBytes());
            SystemMetadata sm = generateSystemMetadata(newid);
            Identifier pid = mn.create(null, newid, is, sm);
            System.out.println("Create completed with PID: " + pid.getValue());

        } catch (BaseException e) {
            e.printStackTrace();
        }
    }

    /**
     * Demonstrate the CNCore.reserveIdentifier() as well as the MNRead.create().
     *
     * Like the above method, this creates a data object that serves no purpose
     * other than to demonstrate how to reserve and create objects.  Don't run
     * this on production servers.
     *
     * @param mnNodeId  Member Node identifier
     */
    private static void runExampleCreate_version2(String mnNodeId) {
        try {
            Identifier newid = new Identifier();
            String idstr = "test2_" + System.currentTimeMillis();
            newid.setValue(idstr);

            // Reserve this identifier, which makes sure it isn't already in use.
            CNode cn = D1Client.getCN();
            cn.reserveIdentifier(newid);

            // Create the object and persist.
            String csv = "1,2,3";
            InputStream is = new ByteArrayInputStream(csv.getBytes());
            SystemMetadata sm = generateSystemMetadata(newid);

            // Get the Member node
            NodeReference nodeRef = new NodeReference();
            nodeRef.setValue(mnNodeId);
            MNode mn = D1Client.getMN(nodeRef);
            Identifier pid = mn.create(null, newid, is, sm);

            System.out.println("Create completed with PID: " + pid.getValue());

        } catch (BaseException e) {
            e.printStackTrace();
        }
    }


    /**
     * Create a SystemMetadata object for the given Identifier, using fake values
     * for the SystemMetadata fields.
     * @param newid the Identifier of the object to be described
     * @return the SystemMetadata object that is created
     */
    private static SystemMetadata generateSystemMetadata(Identifier newid) {
        SystemMetadata sm = new SystemMetadata();
        sm.setIdentifier(newid);
        ObjectFormatIdentifier fmtid = new ObjectFormatIdentifier();
        fmtid.setValue("text/csv");
        sm.setFormatId(fmtid);
        sm.setSize(new BigInteger("5"));
        Checksum cs = new Checksum();
        cs.setAlgorithm("SHA-1");
        cs.setValue("879384579485739487534987");
        sm.setChecksum(cs);
        Subject clientSubject = ClientIdentityManager.getCurrentIdentity();
        sm.setRightsHolder(clientSubject);            
        sm.setSubmitter(clientSubject);
        return sm;
    }
}
