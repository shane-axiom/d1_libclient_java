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
package org.dataone.client.v1.formats;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.ObjectFormat;

/**
 * A singleton class that provides metadata about object formats, such as the MIME
 * type and file extensions associated with a particular format.  Currently, this 
 * information is shipped with this class as a CSV file that is parsed and made
 * available through the accessor methods in this class.  By default, this mapping
 * data is serialized to $HOME/DataONE/mime-mappings.csv, and can be extended by 
 * appending additional mappings onto the end of that CSV file.  The location of
 * the mime-mappings.csv file can be changed by setting the application Setting
 * with the key "D1Client.resourcesDir" to a user-writable directory. 
 */
public class ObjectFormatInfo {
    private static Log log = LogFactory.getLog(ObjectFormatInfo.class);

    /** A map of mime types keyed on formatID. */
    private Map<String, String> mimeMap = null;
    
    /** A map of file extensions keyed on formatID. */
    private Map<String, String> extMap = null;
    
    /** The single instance of this class that should exist. */
    private static ObjectFormatInfo self = null;
    
    /** Estimate of number of Object formats to be created. */
    private static int capacityEstimate = 125;
        
    private static final String MAPPING_FILENAME = "mime-mappings.csv";
    private static final String DEFAULT_RESOURCE_DIR = "DataONE";

    
    /** 
     * Private constructor used to build the singleton ObjectFormatInfo object.
     */
    private ObjectFormatInfo() {
        mimeMap = new HashMap<String, String>(capacityEstimate);
        extMap = new HashMap<String, String>(capacityEstimate);
        loadMappings();
    }
    
    /**
     * Get the single instance of this class, creating it when needed on 
     * first invocation.
     * @return ObjectFormatInfo instance
     */
    public static ObjectFormatInfo instance() {
        if (null == self) {
            self = new ObjectFormatInfo();
        }
        
        return self;
    }
    
    /**
     * Cause the ObjectFormatInfo to reload its data, which enables a calling application
     * to programatically modify the underlying mapping data and then reload the
     * mappings in this class for use without restarting the application.
     */
    public static void reload() {
        self = new ObjectFormatInfo();
    }

    /**
     * Look up the mime type for an ObjectFormat based on the format identifier
     * for that ObjectFormat.
     * @param formatID the identifier of the ObjectFormat to be looked up
     * @return String representation of the mime type
     */
    public String getMimeType(String formatID) {
        return mimeMap.get(formatID);
    }

    /**
     * Look up the mime type for an ObjectFormat.
     * @param format the ObjectFormat to be looked up
     * @return String representation of the mime type
     */
    public String getMimeType(ObjectFormat format) {
        return mimeMap.get(format.getFormatId().getValue());
    }
    
    /**
     * Look up the extension for an ObjectFormat based on the format identifier
     * for that ObjectFormat.
     * @param formatID the identifier of the ObjectFormat to be looked up
     * @return String representation of the extension
     */
    public String getExtension(String formatID) {
        return extMap.get(formatID);
    }

    /**
     * Look up the extension for an ObjectFormat.
     * @param format the ObjectFormat to be looked up
     * @return String representation of the extension
     */
    public String getExtension(ObjectFormat format) {
        return extMap.get(format.getFormatId().getValue());
    }

    /** 
     * Load the mappings from ObjectFormat identifier to mime type and file 
     * extension from disk.
     */
    private void loadMappings() {
        assert(mimeMap != null);
        assert(extMap != null);
                
        try {
            File mappingFile = findMappingFile();
            FileInputStream fis = new FileInputStream(mappingFile);
            parseMappingFile(fis);
        } catch (IOException e) {
            // the default mapping file could not be read or written to disk due to some
            // sort of File I/O problem such as disk full, permissions error, etc.

            // Log a warning that the file could not be created
            log.warn("Problem reading or saving mapping file, so using internal version: " + e.getMessage());

            // Use the default file loaded from the classpath from the jar file
            InputStream mappingStream = ObjectFormatInfo.class.getResourceAsStream(MAPPING_FILENAME);
            try {
                parseMappingFile(mappingStream);
            } catch (IOException e1) {
                // A serious failure, and indicates that the mapping data
                // could not be read even from the internally saved CSV file on the classpath
                // The class can still be instantiated, but there will not be any
                // mapping data
                log.error("Serious error while trying to read mapping data: " + e1.getMessage());
            }
        }
    }

    /**
     * Parse mapping data that is in CSV format to extract the mime type and
     * file extension for each formatID.  The CSV data is assumed to have three
     * columns (formatID,mimeType,extension) separated by commas.   Commas are
     * assumed to not be used elsewhere in the file except as field delimiters.
     * The file is assumed to have a single header row, which is skipped.  The
     * mimeType and extension are saved for later lookup based on formatID.
     * @param is InputStream containing the CSV data to be parsed
     * @throws IOException if there is an error reading the CSV data
     */
    private void parseMappingFile(InputStream is) throws IOException {
        InputStreamReader isr = new InputStreamReader(is, "UTF-8" );
        BufferedReader csv = new BufferedReader(isr);
        
        // Read the first line from the file, then skip it in the loop because it is a header line
        String line = csv.readLine();
        while (line != null) {
            line = csv.readLine();
            if (line != null) {
                // Parse the line to extract the mime type and extension
                String[] fields = line.split(",");
                if (fields.length == 3) {
                    // fields[0] contains the formatID
                    // fields[1] contains the mime type
                    // fields[2] contains the extension
                    mimeMap.put(fields[0], fields[1]);
                    extMap.put(fields[0], fields[2]);
                }
            }
        }
        csv.close();
    }
    
    /**
     * Determine the location of the mime-type mapping file on this system. The mime-type mapping 
     * file is a CSV-formatted file used to map the ObjectFormat formatid to an associated mime-type
     * and filename extension for that object format. If the mapping file is not found 
     * in the resources directory indicated in the Settings, then create it from a classpath
     * loaded resource.  If the resources directory is not set, then create one in a default
     * location and create the mapping file in that default directory. 
     * @throws IOException if the mapping file can not be created on disk
     * @return File pointing to the mapping file
     */
    private File findMappingFile() throws IOException {
        
        File mappingFile = null;
        
        // Determine which local dir might be used to store resources
        String resourcesDirName = Settings.getConfiguration().getString("D1Client.resourcesDir");
        
        if (resourcesDirName == null) {
            String home = System.getProperty("user.home");
            File resourcesDir = new File(home, DEFAULT_RESOURCE_DIR);
            mappingFile = getOrCreateMappingFile(resourcesDir);
        } else {
            // Check if a mappings file exists in the configured resources directory
            File resourcesDir = new File(resourcesDirName);
            mappingFile = getOrCreateMappingFile(resourcesDir);
        }       
                       
        return mappingFile;
    }

    /**
     * If the mapping file is not found in the resourcesDir directory parameter, 
     * then create it from a classpath loaded resource.
     * 
     * @param resourcesDir File pointing to the resources directory to check for a mappings file
     * @throws IOException if the mapping file can not be created on disk
     * @return File pointing to the mapping file
     */
    private File getOrCreateMappingFile(File resourcesDir) throws IOException {
        File mappingFile = null;

        mappingFile = new File(resourcesDir, MAPPING_FILENAME);
        if (mappingFile.exists()) {
            return mappingFile;
        } else {
            InputStream mappingStream = ObjectFormatInfo.class.getResourceAsStream(MAPPING_FILENAME);
            if (!resourcesDir.exists()) {
                boolean success = resourcesDir.mkdirs();
            }
            mappingFile.createNewFile();
            FileOutputStream mappingFileStream = new FileOutputStream(mappingFile);
            IOUtils.copy(mappingStream, mappingFileStream);
            mappingFileStream.close();
            return mappingFile;
        }
    }
}
