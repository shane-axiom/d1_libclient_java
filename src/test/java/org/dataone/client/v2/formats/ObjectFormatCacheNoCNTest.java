package org.dataone.client.v2.formats;

import static org.junit.Assert.*;

import java.util.Date;

import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ObjectFormatCacheNoCNTest {

	private static String cnUrl = null;
	private static int throttle;
	
	@BeforeClass
	public static void clearCnUrl() {
		cnUrl = Settings.getConfiguration().getString("D1Client.CN_URL");
        Settings.getConfiguration().setProperty("D1Client.CN_URL", "");
        throttle = Settings.getConfiguration().getInt("ObjectFormatCache.minimum.refresh.interval.seconds");
        Settings.getConfiguration().setProperty("ObjectFormatCache.minimum.refresh.interval.seconds", 1);
        

	}
	@AfterClass
	public static void resetCnUrl() {
        Settings.getConfiguration().setProperty("D1Client.CN_URL", cnUrl);
        Settings.getConfiguration().setProperty("ObjectFormatCache.minimum.refresh.interval.seconds", throttle);
	}
	
    @Test
    public void testRefreshCache_blankCN_URL_noException()  {
        try {
            ObjectFormatCache ofc = ObjectFormatCache.getInstance();
            ofc.refreshCache();
        } catch (ServiceFailure e) {
            e.printStackTrace();
            fail("refreshCache should not throw exceptions if there are no CNs");
        }
    }
    
    @Test
    public void testRefreshCache_blankCN_URL()  {
        try {
            ObjectFormatCache ofc = ObjectFormatCache.getInstance();
            int origSize = ofc.getObjectFormatMap().size();
            ofc.refreshCache();
            int newSize = ofc.getObjectFormatMap().size();
            assertTrue("ObjectFormatMap should not decrease in size", newSize >= origSize);
        } catch (ServiceFailure e) {
            e.printStackTrace();
            fail("refreshCache should not throw exceptions if there are no CNs");
        }
    }
    
    @Test
    public void testDefaultMapNotEmpty()  {
        try {
            ObjectFormatCache ofc = ObjectFormatCache.getInstance();
            int origSize = ofc.getObjectFormatMap().size();
            assertTrue("ObjectFormatMap should not be empty", origSize > 0);
        } catch (RuntimeException e) {
            e.printStackTrace();
            fail("refreshCache should not throw exceptions if there are no CNs");
        }
    }
    
    // this test assumes that the CN has not been reached, so we are in fallback mode
    @Test 
    public void testUsingDefaultStrategy() throws ServiceFailure {
        assertTrue("Should still be in fallback mode", ObjectFormatCache.getInstance().isUsingFallbackFormatList());
    }
    
    // this test assumes that the CN has not been reached, so we are in fallback mode
    @Test
    public void testGetLastRefreshDate() throws ServiceFailure {
        assertTrue("Should be date 0",
                ObjectFormatCache.getInstance().getLastRefreshDate().getTime() == (new Date(0)).getTime());
    }

}
