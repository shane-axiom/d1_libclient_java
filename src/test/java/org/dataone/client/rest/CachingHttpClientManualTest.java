package org.dataone.client.rest;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Date;

import org.dataone.client.exception.ClientSideException;
import org.dataone.client.v2.CNode;
import org.dataone.client.v2.itk.D1Client;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.InsufficientResources;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.TypeFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class CachingHttpClientManualTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    /**
     * test illustrates that the cache is
     * @throws IOException
     * @throws ClientSideException
     * @throws BaseException
     */
    @Ignore("because it relies on external resources")
    @Test
    public void callsShouldNotFailIfCacheFull() throws IOException, ClientSideException, BaseException {
//       Settings.getConfiguration().setProperty("D1Client.http.cacheMaxEntries", 3);
        MultipartRestClient mrc = new DefaultHttpMultipartRestClient();
        mrc.doGetRequest("https://cn-dev-ucsb-1.test.dataone.org/cn/v2/node", 30000);
        mrc.doGetRequest("https://cn-dev-ucsb-1.test.dataone.org/cn/v1/node", 30000);
        mrc.doGetRequest("https://cn-dev-ucsb-1.test.dataone.org/cn/v2/node", 30000);
    }
    
    /**
     * this manual test depends on enabling http debugging and monitoring
     * stdout to see how many calls are made to the server.
     * 
     * With cn.ping() notice that it is not cached...
     * @throws ServiceFailure
     * @throws NotImplemented
     * @throws InsufficientResources
     * @throws InterruptedException
     */
    @Ignore("no asserts and reliance on external resources, so only for manual run")
    @Test
    public void cacheShouldOnlyReloadObjectAfterExpiration() throws ServiceFailure, NotImplemented, InsufficientResources, InterruptedException {
        CNode cn = D1Client.getCN("https://cn-dev-ucsb-1.test.dataone.org/cn");
        
        int sleepSec = 0;
        int totalSleep =0;
        for (int i = 0; i < 10; i++) {
            Date d = cn.ping();
//            Date d = new Date();
            System.out.printf("********PING: %d. [%d / %d] %s...\n",i,sleepSec,totalSleep, d.toString());
            cn.listNodes();
            sleepSec = 3 + i;
            totalSleep += sleepSec;
            Thread.sleep((3 + i)*1000);
        }
    }
    
    @Ignore("no asserts and reliance on external resources, so only for manual run")
    @Test
    public void cacheShouldNotCacheNonCachableObjects() throws ServiceFailure, NotImplemented, InsufficientResources, InterruptedException {
        CNode cn = D1Client.getCN("https://cn-dev-ucsb-1.test.dataone.org/cn");
        System.out.printf("starting ping test...\n");
        Date d = cn.ping();
        Thread.sleep(500);
        d = cn.ping();
    }
}
