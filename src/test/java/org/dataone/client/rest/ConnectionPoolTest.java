package org.dataone.client.rest;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.Executor;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.exception.ClientSideException;
import org.dataone.service.exceptions.BaseException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class ConnectionPoolTest {

    protected static Log logger = LogFactory.getLog(ConnectionPoolTest.class);
    
    static MultipartRestClient singleRestClient;
    
    
    class RestCallRunnable implements Runnable {

        private URL url;
        private MultipartRestClient mrc;
        
        @Override
        public void run() {
            InputStream is = null;
            System.out.println("Running request");
            try {
                is = mrc.doGetRequest(url.toExternalForm(), 10000);
                System.out.println("made request " + url.toExternalForm());
//                logger.info(IOUtils.toString(is, "UTF-8").substring(100, 300) + "...");
//                IOUtils.copy(is, System.out);
//                Thread.sleep(2000);
            } catch (BaseException | ClientSideException e) { //| IOException e) {
                logger.info(String.format("yielded exception: %s %s",
                        e.getClass().getName(),
                        e.getMessage()));
//            } catch (InterruptedException e) {
//                logger.warn("Interrupted",e);
//            } finally{
//                IOUtils.closeQuietly(is);
//                
//                logger.info(String.format("call: %s",
//                        mrc.getLatestRequestUrl()));
            }
        }
        
        RestCallRunnable(MultipartRestClient rc, URL url) {
            this.url = url;
            this.mrc = rc;
        }
        
    }
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        singleRestClient = new DefaultHttpMultipartRestClient();
        
    }

    
    @Ignore("no asserts, and there are dependencies on network resources...")
    @Test
    public void testConnectionPool_shouldNotGet9thRequestOff() throws MalformedURLException, InterruptedException {
        Thread t1 = new Thread(new RestCallRunnable(
                singleRestClient,new URL("https://cn-sandbox.test.dataone.org/cn/v1/node#1")));
        Thread t2 = new Thread(new RestCallRunnable(
                singleRestClient,new URL("https://cn-sandbox.test.dataone.org/cn/v1/formats#2")));
        Thread t3 = new Thread(new RestCallRunnable(
                singleRestClient,new URL("https://cn-sandbox.test.dataone.org/cn/v1/accounts#3")));
        Thread t4 = new Thread(new RestCallRunnable(
                singleRestClient,new URL("https://cn-sandbox.test.dataone.org/cn/v1/node#4")));
        Thread t5 = new Thread(new RestCallRunnable(
                singleRestClient,new URL("https://cn-sandbox.test.dataone.org/cn/v1/formats#5")));
        Thread t6 = new Thread(new RestCallRunnable(
                singleRestClient,new URL("https://cn-sandbox.test.dataone.org/cn/v1/accounts#6")));

        Thread t7 = new Thread(new RestCallRunnable(
                singleRestClient,new URL("https://cn-sandbox.test.dataone.org/cn/v1/node#7")));
        Thread t8 = new Thread(new RestCallRunnable(
                singleRestClient,new URL("https://cn-sandbox.test.dataone.org/cn/v1/formats#8")));
        Thread t9 = new Thread(new RestCallRunnable(
                singleRestClient,new URL("https://cn-sandbox.test.dataone.org/cn/v1/accounts#9")));

        System.out.println("starting threads...");
        t1.start();
        t2.start();
        t3.start();
        t4.start();
        t5.start();
        t6.start();
        t7.start();
        t8.start();
        t9.start();
        
        int liveThreads = 9;
        while (liveThreads > 0) {
            Thread.sleep(1000);
            liveThreads = 0;
            
            if (!t1.isAlive())
                logger.info(t1.getName() + " (t1) died");
            else
                liveThreads++;
           
            if (!t2.isAlive())
                logger.info(t2.getName() + " (t2) died");
            else
                liveThreads++;
            
            if (!t3.isAlive())
                logger.info(t3.getName() + " (t3) died");
            else
                liveThreads++;
            
            if (!t4.isAlive())
                logger.info(t4.getName() + " (t4) died");
            else
                liveThreads++;
           
            if (!t5.isAlive())
                logger.info(t5.getName() + " (t5) died");
            else
                liveThreads++;
            
            if (!t6.isAlive())
                logger.info(t6.getName() + " (t6) died");
            else
                liveThreads++;
            
            if (!t7.isAlive())
                logger.info(t7.getName() + " (t7) died");
            else
                liveThreads++;
           
            if (!t8.isAlive())
                logger.info(t8.getName() + " (t8) died");
            else
                liveThreads++;
            
            if (!t9.isAlive())
                logger.info(t9.getName() + " (t9) died");
            else
                liveThreads++;
        }
    }
}
