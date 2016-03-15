package org.dataone.client.rest;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.exception.ClientSideException;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.BaseException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class IdleConnectionMonitoringTest {

    protected static Log logger = LogFactory.getLog(IdleConnectionMonitoringTest.class);
    
    static List<Object> objects = new LinkedList<>();
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        Settings.getConfiguration().setProperty("D1Client.http.monitorIdleConnections", true);
    }

    @Ignore("there are native calls and calls to other servers in this test")
    @Test
    public void testGarbageCollectionThreadCompletion() throws IOException, ClientSideException, InterruptedException {
        // start up a bunch of connection managers with attached monitor threads
        // , make a couple calls, deference them, then wait until they are garbage collected.
        
        int before = runNetstat();

        for (int i=0; i<5; i++) {
            MultipartRestClient mrc= new DefaultHttpMultipartRestClient();
            URL urlA = new URL("https://cn-sandbox.test.dataone.org/cn/v1/node#"+i);
            URL urlB = new URL("https://cn-sandbox.test.dataone.org/cn/v1/formats#"+i);
            
            InputStream isA = null;
            InputStream isB = null;
            System.out.println("Running requests for mrc " + i);
            try {
                isA = mrc.doGetRequest(urlA.toExternalForm(), 10000);
                System.out.println("made request " + urlA.toExternalForm());
                IOUtils.toByteArray(isA);

                isB = mrc.doGetRequest(urlB.toExternalForm(), 10000);
                System.out.println("made request " + urlB.toExternalForm());
                IOUtils.toByteArray(isB);

//                logger.info(IOUtils.toString(is, "UTF-8").substring(100, 300) + "...");
//                IOUtils.copy(is, System.out);
//                Thread.sleep(2000);
            } catch (BaseException | ClientSideException e) { //| IOException e) {
                System.out.println(String.format("yielded exception: %s %s",
                        e.getClass().getName(),
                        e.getMessage()));
//            } catch (InterruptedException e) {
//                logger.warn("Interrupted",e);
            } finally{
                IOUtils.closeQuietly(isA);
                IOUtils.closeQuietly(isB);
                System.out.println(String.format("call: %s",
                        mrc.getLatestRequestUrl()));
            }
            
        }
        System.out.println("====================== BEFORE ================");
        int beforeGC = runNetstat();
        Thread.sleep(100);
        System.gc();
        
        Thread.sleep(5000);
        int after = 0;
        for (int i=0; i<3; i++) {
            Thread.sleep(5000);
            System.out.println("====================== " + i + " ================");
            after = runNetstat();
            if (after == before) 
                break;
        }
        assertTrue("Should have the inital number of CLOSE_WAITS after 3+ periods of monitoring",after == before);
    }
    
    private int runNetstat() throws IOException {
        Process process = new ProcessBuilder(
                "/usr/sbin/netstat").start();

        InputStream is = process.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        String line;
        int c = 0;
        while ((line = br.readLine()) != null) {
            if (line.contains("CLOSE_WAIT")) { 
                System.out.println(line);
                c++;
            } else if( line.contains("ESTABLISHED")) {
                System.out.println(line);
            }
        }
        return c;
    }
}
