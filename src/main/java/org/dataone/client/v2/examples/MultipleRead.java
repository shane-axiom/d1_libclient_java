package org.dataone.client.v2.examples;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.io.IOUtils;
import org.dataone.client.v2.CNode;
import org.dataone.client.v2.itk.D1Client;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Identifier;
import org.w3c.dom.Document;


public class MultipleRead {

	public static void main(String[] args) {
       
		
		// use SANDBOX2
    	Settings.getConfiguration().setProperty("D1Client.CN_URL", "https://cn-sandbox-2.test.dataone.org/cn");
		 
		List<String> identifiers = new ArrayList<String>();

		System.out.println("Starting MultipleRead.main()");
		InputStream inputStream = null;
		try {
        	// gather the pids to test reading
//	        if (args.length > 0) {
	        	// read the pids from this file URL
//	        	String pidFile = args[0];
	        	URL url = new URL("https://raw.githubusercontent.com/DataONEorg/semantic-query/master/lib/test_corpus_C_id_list.txt");
	        	inputStream = url.openStream();
	    		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
	    		String line;
	    		while ((line = reader.readLine()) != null) {
	    			identifiers.add(line);
	    		}
//	        }
	        
        } catch (Exception e) {
        	e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
	       
		System.out.println("%%%%%%%%%%%%% getting all objects %%%%%%%%%%%%%%");
        // read em all
        int count = 0;
        try {
            CNode cn = D1Client.getCN();
            for (String id: identifiers) {
                InputStream emlStream = null;

                try {
                    Identifier pid = new Identifier();
                    pid.setValue(id);
                    emlStream = cn.get(null, pid);
                    //				DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                    //		        Document doc = builder.parse(emlStream);
                    System.out.println(IOUtils.toString(emlStream, "UTF-8").substring(0, 80) + "...");
                    count++;
                    System.out.println("Read count: " + count);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    IOUtils.closeQuietly(emlStream);
                }
            }
        } catch (NotImplemented | ServiceFailure e) {
            e.printStackTrace();
        }
        
	}
}
