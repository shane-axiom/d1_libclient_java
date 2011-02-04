package org.dataone.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Settings {
    private static final String BUNDLE_NAME = "/d1client.properties";
    private static Properties props = null;
    
    static {
        // initialize resource bundle
        Settings.initializeSettings(BUNDLE_NAME);
    }

    /**
     * A private constructor to be sure no instances are created.
     */
    private Settings() {
    }
    
    /**
     * Load the properties from a Java properties file from the classpath, setting each
     * property as needed.
     * @param BUNDLE_NAME
     * @param settingsClass
     */
    private static void initializeSettings(String BUNDLE_NAME) {
        InputStream propertiesStream = (Settings.class).getResourceAsStream(BUNDLE_NAME);
        props = new Properties();
        try {
            props.load(propertiesStream);
                        
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Return one of the properties based on its key value.
     * @param key the String name of a property key
     * @return the value of that property
     */
    public static String get(String key) {
        return props.getProperty(key);
    }
}

