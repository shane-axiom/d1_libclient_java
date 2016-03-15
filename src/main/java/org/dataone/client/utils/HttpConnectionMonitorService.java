package org.dataone.client.utils;

import java.io.Closeable;
import java.util.Map.Entry;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.log4j.Logger;

/**
 * A Service class that monitors HttpClient ConnectionManagers for stale (idle
 * and expired) connections and removes them to free up system resources (sockets).
 * 
 * Because HttpClients only close their end of a socket when they discover the 
 * socket closed upon the next request, the only two reliable ways to release sockets
 * are to (a) monitor their ConnectionManagers in a separate thread, and to (b)
 * actively shutdown the ConnectionManager or close the HttpClient.
 * <p/>
 * The D1Client facades hide HttpClients from users, so this class is also used
 * to actively shutdown all of the instantiated and registered ConnectionManagers
 * at the interruption of it's run() method.  
 * <p/>
 * Assuming that this thread runs for the duration of the application run, this
 * ensures that sockets created and not stale get released when the program exits.
 * 
 * @author rnahf
 *
 */
public class HttpConnectionMonitorService extends Thread {

    WeakHashMap<HttpClientConnectionManager, String> connManStateMap = new WeakHashMap<>();
    WeakHashMap<HttpClient, String> clientStateMap = new WeakHashMap<>();
   
    private volatile boolean shutdown;

    final static Logger logger = Logger.getLogger(HttpConnectionMonitorService.class);
    
    private static class SingletonHolder {
        public static final HttpConnectionMonitorService INSTANCE = new HttpConnectionMonitorService();
        public static final Thread theMonitorThread = new Thread(INSTANCE);
        static {
            theMonitorThread.start();
            logger.warn("Starting monitor thread");
        }
        
    }
    
    public static HttpConnectionMonitorService getInstance() {
        return SingletonHolder.INSTANCE;
    }


    private HttpConnectionMonitorService() { }


    
    /** 
     * Add a connectionManager to monitor
     * @param cMan
     */
    public void addMonitor(HttpClientConnectionManager cMan) {
        logger.warn("registering ConnectionManager...");
        connManStateMap.put(cMan, "REGISTERED");
    }
    
    /**
     * Add an HttpClient to close upon exit.  HttpClients don't give access
     * to the ConnectionManager, so we cannot close stale connections, but we
     * can call close() upon exit, to release system resources.
     * @param client
     */
    public void addHttpClientMonitor(HttpClient client) {
        clientStateMap.put(client, "REGISTERED");
    }
    
    /**
     * remove a ConnectionManager from monitoring
     * Not sure of the use case for it, since we need to keep all of the
     * ConnectionManagers around until application shutdown, where-upon 
     * we close all connection managers.
     * @param cMan
     */
    public void removeMonitor(HttpClientConnectionManager cMan) {
        connManStateMap.put(cMan,"REMOVED");
    }

    WeakHashMap<HttpClientConnectionManager,String> getMonitors() {
        return this.connManStateMap;
    }
    
    @Override
    public void run() {
        logger.warn("Starting monitoring...");
        try {
            while (!shutdown) {
                
                synchronized (this) {
                    wait(5000);
                    for (Entry<HttpClientConnectionManager,String> n : connManStateMap.entrySet()) {
                        logger.debug("...calling closeExpire/IdleConnections...");
                        try {
                            // Close expired connections
                            n.getKey().closeExpiredConnections();
                            // Optionally, close connections
                            // that have been idle longer than 30 sec
                            n.getKey().closeIdleConnections(30, TimeUnit.SECONDS);

                        } catch (NullPointerException e) {
                            // if the connectionManager is GC'ed while using it
                            // there is no problem because finalize was called and there
                            // would be no connections to worry about anymore.
                            logger.info("ConnectionManager went out of scope.");
//                            notifyAll();
                        }
                    }
                }
            }
        } catch (InterruptedException ex) {
            //this runnable is exiting...
            shutdownConnectionManagers();
            // fwiw, reset the interrupt on this thread in case others care
            Thread.currentThread().interrupt();
            
        } finally {
            logger.warn("Exiting HttpConnectionsMonitorService...");
        }
    }

    protected void shutdownConnectionManagers() {
        shutdown = true;
        synchronized (this) {
            logger.warn(String.format("Found %d registered ConnectionManagers to shutdown.",
                    connManStateMap.size()));
            logger.warn("Shutting down all registered ConnectionManagers!!");
            for (Entry<HttpClientConnectionManager,String> n : connManStateMap.entrySet()) {
                n.getKey().shutdown();
            }
            logger.warn("Shutting down all registered HttpClients!!");
            for (Entry<HttpClient,String> n : clientStateMap.entrySet()) {
                if (n.getKey() instanceof Closeable) {
                    IOUtils.closeQuietly(((Closeable)n.getKey()));
                }
            }            
            notifyAll();
        }
    }


}
