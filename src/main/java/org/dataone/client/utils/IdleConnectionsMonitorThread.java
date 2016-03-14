package org.dataone.client.utils;

import java.lang.ref.WeakReference;
import java.util.concurrent.TimeUnit;

import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.log4j.Logger;

/**
 * A class that monitors the ConnectionManager to close expired and idle threads
 * It maintains only a WeakReference to the ConnectionManager can be garbage
 * collected.  (GC calls connectionManager.finalize() which shuts down and
 * releases all resources).
 * 
 * @author rnahf
 *
 */
class IdleConnectionsMonitorThread extends Thread {

    private final WeakReference<HttpClientConnectionManager> weakConnMgrRef;
    private volatile boolean shutdown;

    final static Logger logger = Logger.getLogger(IdleConnectionsMonitorThread.class);

    public IdleConnectionsMonitorThread(HttpClientConnectionManager connMgr) {
        super();
        // we keep only a weak reference to the connection manager so it can
        // be garbage collected
        this.weakConnMgrRef = new WeakReference<HttpClientConnectionManager>(connMgr);
        
        logger.warn("Starting Idle Connections Monitor...");
    }

    @Override
    public void run() {
        try {
            while (!shutdown) {
                synchronized (this) {
                    
                    wait(5000);
                    logger.debug("...calling closeExpire/IdleConnections...");
                    try {
                        // Close expired connections
                        weakConnMgrRef.get().closeExpiredConnections();
                        // Optionally, close connections
                        // that have been idle longer than 30 sec
                        weakConnMgrRef.get().closeIdleConnections(30, TimeUnit.SECONDS);
                        
                    } catch (NullPointerException e) {
                        // thrown if weakConnMgrRef.get() yields null
                        logger.warn("ConnectionManager out of scope. Shutting down Idle Connections Monitor...");
                        notifyAll();
                        break;
                    }
                }
            }
        } catch (InterruptedException ex) {
            // terminate
        } finally {
            // any cleanup?
        }
    }

    public void shutdown() {
        shutdown = true;
        synchronized (this) {
            logger.warn("Shutting down Idle Connections Monitor...");
            notifyAll();
        }
    }
}

