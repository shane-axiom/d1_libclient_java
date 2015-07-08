package org.dataone.client.v2.formats;

import static org.junit.Assert.*;

import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ObjectFormatCacheTest {

	private static String cnUrl = null;
	
	@BeforeClass
	public static void clearCnUrl() {
		cnUrl = Settings.getConfiguration().getString("D1Client.CN_URL");
        Settings.getConfiguration().setProperty("D1Client.CN_URL", "");

	}
	@AfterClass
	public static void resetCnUrl() {
        Settings.getConfiguration().setProperty("D1Client.CN_URL", cnUrl);
	}
	
    @Test
    public void testRefreshCache_blankCN_URL_noException()  {
        try {
            ObjectFormatCache ofc = ObjectFormatCache.getInstance();
            ofc.refreshCache();
        } catch (ServiceFailure e) {
            e.printStackTrace();
            fail("refreshCache should not throw exceptions if there are no CNs");
        } catch (NotImplemented e) {
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
        } catch (NotImplemented e) {
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
        } catch (ServiceFailure e) {
            e.printStackTrace();
            fail("refreshCache should not throw exceptions if there are no CNs");
        }
    }

}
