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

public class MultithreadedRestClientTest {

    protected static Log logger = LogFactory.getLog(MultithreadedRestClientTest.class);
    
    static MultipartRestClient singleRestClient;
    
    
    class RestCallRunnable implements Runnable {

        private URL url;
        private MultipartRestClient mrc;
        
        @Override
        public void run() {
            InputStream is = null;
            try {
                is = mrc.doGetRequest(url.toExternalForm(), 10000);
                logger.info(IOUtils.toString(is, "UTF-8").substring(100, 300) + "...");
//                IOUtils.copy(is, System.out);
                Thread.sleep(2000);
            } catch (BaseException | ClientSideException | IOException e) {
                logger.info(String.format("yielded exception: %s %s",
                        e.getClass().getName(),
                        e.getMessage()));
            } catch (InterruptedException e) {
                logger.warn("Interrupted",e);
            } finally{
                IOUtils.closeQuietly(is);
                
                logger.info(String.format("call: %s",
                        mrc.getLatestRequestUrl()));
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
    public void testLatestUrl() throws MalformedURLException, InterruptedException {
        Thread t1 = new Thread(new RestCallRunnable(
                singleRestClient,new URL("https://cn-sandbox.test.dataone.org/cn/v1/node")));
        Thread t2 = new Thread(new RestCallRunnable(
                singleRestClient,new URL("https://cn-sandbox.test.dataone.org/cn/v1/formats")));
        Thread t3 = new Thread(new RestCallRunnable(
                singleRestClient,new URL("https://cn-sandbox.test.dataone.org/cn/v1/accounts")));

        t1.start();
//        Thread.sleep(2000);
        t2.start();
//        Thread.sleep(2000);
        t3.start();
        
        int liveThreads = 3;
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
        }
    }
}
