package org.dataone.client.types;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.dataone.client.rest.RestClient;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class AutoCloseHttpClientInputStreamTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    /**
     * when using the AutoCloseHttpClientInputStream, we should see a new socket
     * created in the 'DURING' report, and disappearing in the 'AFTER' report.
     * 
     * The RestClient should return an IllegalStateException if called again.
     * 
     * @throws ClientProtocolException
     * @throws IOException
     * @throws InterruptedException
     */
    @Ignore("this is a manual teset containing external resources.")
    @Test
    public void test() throws ClientProtocolException, IOException, InterruptedException {
        
        System.out.println("====================== BEFORE ================");
        int beforeGC = runNetstat();
        Thread.sleep(200);
        
        
        RestClient rc = new RestClient(HttpClients.createMinimal());
        HttpResponse resp = rc.doGetRequest("https://cn-dev-orc-1.test.dataone.org/cn/v1/node", null);
        HttpEntity n = resp.getEntity();
        
        InputStream is = new AutoCloseHttpClientInputStream(n.getContent(),rc.getHttpClient());
//        InputStream is = n.getContent();

        System.out.println("====================== DURING ================");
        int duringGC = runNetstat();
        
        
        System.out.println(IOUtils.toString(is));
        is.close();
        Thread.sleep(5200);
        System.out.println("====================== AFTER ================");
        int after = runNetstat();
        


        try {
            resp = rc.doGetRequest("https://cn-dev-orc-1.test.dataone.org/cn/v2/node", null);
            EntityUtils.consume(resp.getEntity());
            fail("Should not be able to do a second request from the RestClient");
            
        } catch (IllegalStateException e) {
            ;
        }
    
//     assertTrue("Should have the inital number of CLOSE_WAITS after 3+ periods of monitoring",after == before);
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
            if (line.contains("192.168") && !line.contains("-in-f"))
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
