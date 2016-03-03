package org.dataone.client.utils;

import java.util.concurrent.TimeUnit;

import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.log4j.Logger;

class IdleConnectionsMonitorThread extends Thread {

    private final HttpClientConnectionManager connMgr;
    private volatile boolean shutdown;

    final static Logger logger = Logger.getLogger(IdleConnectionsMonitorThread.class);


    public IdleConnectionsMonitorThread(HttpClientConnectionManager connMgr) {
        super();
        this.connMgr = connMgr;
        logger.warn("Starting Idle Connections Monitor...");
    }

    @Override
    public void run() {
        try {
            while (!shutdown) {
                synchronized (this) {

                    wait(5000);
                    logger.debug("...calling closeExpire/IdleConnections...");
                    // Close expired connections
                    connMgr.closeExpiredConnections();
                    // Optionally, close connections
                    // that have been idle longer than 30 sec
                    connMgr.closeIdleConnections(30, TimeUnit.SECONDS);

                }
            }
        } catch (InterruptedException ex) {
            // terminate
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

