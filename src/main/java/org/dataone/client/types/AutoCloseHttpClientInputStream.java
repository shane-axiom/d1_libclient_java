package org.dataone.client.types;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.input.AutoCloseInputStream;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;

/**
 * Use this class to wrap input streams that come from HttpClients IFF if you
 * plan to never use the HttpClient again. This extended class will automatically
 * closes the input stream AS WELL AS the associated HttpClient once the last byte is read.
 * 
 * Upon close, it also dereferences the HttpClient property to allow it to go
 * out of scope and be eligible for garbage collection.  (Note, other references
 * to the HttpClient the application might have are still the user's responsibility).
 * 
 * @author rnahf
 *
 */
public class AutoCloseHttpClientInputStream extends AutoCloseInputStream {

        private InputStream is;
        private HttpClient hc;
        
        /**
         * 
         * @param is - the InputStream being proxied
         * @param hc - the disposable HttpClient from where the InpuStream came from
         */
        public AutoCloseHttpClientInputStream(InputStream is, HttpClient hc) {
            super(is);
            this.hc = hc;
        }
        
        @Override
        protected void afterRead(final int n) throws IOException {
            if (n == -1) {
                close();
            }
        }
        
        /**
         * Closes the proxied InputStream, and the HttpClient.  Also sets the 
         * HttpClient property to null to help dereference it.
         */
        @Override
        public void close() throws IOException {
            try {
                super.close();
            } catch (IOException e) {
                ;
            } finally {
                if (this.hc instanceof Closeable) {
                    ((Closeable)this.hc).close();
                }
                this.hc = null;
            }
        }
        
        @Override
        protected void finalize() throws Throwable {
            close();
            super.finalize();
        }
    }