package org.dataone.client.datastore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Hashtable;

import org.apache.commons.io.IOUtils;
import org.dataone.service.types.v1.Identifier;


/**
 * A Data store which stores the data objects as the byte arrays.
 * @author tao
 *
 */
public class MemoryDataStore implements DataStore {
    private Hashtable<Identifier, byte[]> store= new Hashtable<Identifier, byte[]>();
    
    /**
     * Get a data object in the InputStream format for a specified identifier. If no matched identifier has been found,
     * null will be returned.
     * @param identifier  the identifier of the data object
     * @return the data object in the InputStream format
     * @throws IOException
     */
    public InputStream get(Identifier identifier) throws IOException{
        byte[] data = store.get(identifier);
        InputStream input = null;
        if (data != null) {
            input = new ByteArrayInputStream(data);
        }
        return input;
    }
    
    /**
     * Store the data object (in the InputSTream format) into the data store with the specified id.
     * @param identifier  the identifier of the data object
     * @param data  the data object as the InputStream object
     * @throws Exception
     */
    public void set(Identifier identifier, InputStream data) throws IOException, NullPointerException {
        byte[] dataArray = IOUtils.toByteArray(data);
        store.put(identifier, dataArray);
    }
    
}
