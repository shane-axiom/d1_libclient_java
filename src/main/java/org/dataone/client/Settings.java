package org.dataone.client;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationFactory;

public class Settings {
    private static Configuration configuration = null;

    /**
     * A private constructor to be sure no instances are created.
     */
    private Settings() {
    	
    }
        
    /**
     * Get a Configuration interface  
     * @return the value of that property
     */
    public static Configuration getConfiguration() {
        if (configuration == null) {
        	try {
            	ConfigurationFactory factory = new ConfigurationFactory("config.xml");
    			configuration = factory.getConfiguration();
    		} catch (ConfigurationException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
        }
        return configuration;
    }
}

