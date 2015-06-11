package org.dataone.client.v2.examples;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.dataone.client.v2.itk.D1Client;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.w3c.dom.Document;


public class MultipleRead {

	public static void main(String[] args) {
       
		
		// use SANDBOX2
    	Settings.getConfiguration().setProperty("D1Client.CN_URL", "https://cn-sandbox-2.test.dataone.org/cn");
		 
		List<String> identifiers = new ArrayList<String>();

		try {
	
        	// gather the pids to test reading
	        if (args.length > 0) {
	        	// read the pids from this file URL
	        	String pidFile = args[0];
	        	URL url = new URL(pidFile);
	        	InputStream inputStream = url.openStream();
	    		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
	    		String line;
	    		while ((line = reader.readLine()) != null) {
	    			identifiers.add(line);
	    		}
	        }
	        
        } catch (Exception e) {
        	e.printStackTrace();
        }
	       
        // read em all
        int count = 0;
        for (String id: identifiers) {
        	try {
	    		Identifier pid = new Identifier();
	    		pid.setValue(id);
				InputStream emlStream = D1Client.getCN().get(null, pid);
//				DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
//		        Document doc = builder.parse(emlStream);
		        emlStream.close();
		        count++;
		        System.out.println("Read count: " + count);
        	} catch (Exception e) {
            	e.printStackTrace();
            }
				  
        }
        
	}
}
