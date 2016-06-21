package org.dataone.client.v1.examples;

import java.util.List;

import org.dataone.client.v2.CNode;
import org.dataone.client.v2.MNode;
import org.dataone.client.v2.itk.D1Client;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Identifier;

public class ExampleObjectDownload {

    /**
     * @param args
     */
    public static void main(String[] args) {
       
        try {
            System.out.println(Settings.getConfiguration().getProperty("D1Client.CN_URL"));
            CNode sourceCN = D1Client.getCN();
            System.out.println("Done.");
        } catch (ServiceFailure | NotImplemented e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
