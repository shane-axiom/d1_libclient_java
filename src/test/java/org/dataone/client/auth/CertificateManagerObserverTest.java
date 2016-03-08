package org.dataone.client.auth;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Observable;
import java.util.Observer;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CertificateManagerObserverTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void test() throws IOException, InterruptedException {
        
        final List<String> observations = new LinkedList<>();
        
        Observer obs = new Observer() {

            int i = 0;
            
            @Override
            public void update(Observable o, Object arg) {
                observations.add("observation: " + i++);
            }
        };


        CertificateManager cm = CertificateManager.getInstance();
        cm.addObserver(obs);
        cm.setCertificateLocation(null);
        assertTrue("1. Should not have received a notification when changing from null to null", observations.size()==0);
        cm.setCertificateLocation("/usr/local/fake");
        assertTrue("2. Should have received a notification when changing from null to some fake location", observations.size()==1);
        cm.setCertificateLocation("/usr/local/fake");
        assertTrue("3. Should not have received a notification when changing from some fake location to the same fake location", observations.size()==1);
        cm.setCertificateLocation("/usr/local/fake2");
        assertTrue("4. Should receive a notification when changing from some fake location to a different fake location", observations.size()==2);
        cm.setCertificateLocation(null);
        assertTrue("5. Should receive a notification when changing from some fake location to null", observations.size()==3);
        
        File outputFile = File.createTempFile("certMan.test.", null);
        OutputStreamWriter osw = null;
        FileOutputStream os = null;
        try {
            os = new FileOutputStream(outputFile);
            osw = new OutputStreamWriter(os,"UTF-8");
            osw.write("startingValue\n");
        } finally {
            osw.flush();
            osw.close();
            os.flush();
            os.close();
        }

        
        String testFilePath = outputFile.getCanonicalPath();
        Thread.sleep(30);
        cm.setCertificateLocation(testFilePath);
        assertTrue("6. Should receive a notification when changing from null to a real file", observations.size()==4);
        
        Thread.sleep(30);
        cm.setCertificateLocation(testFilePath);
        assertTrue("7. Should NOT receive a notification when changing a real file to the same real file", observations.size()==4);
        
        Thread.sleep(30);
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(outputFile);
            osw = new OutputStreamWriter(fos,"UTF-8");
            osw.write("updatedContent\n");
        } finally {
            osw.flush();
            osw.close();
            fos.flush();
            fos.close();
        } 
        Thread.sleep(30);
        cm.setCertificateLocation(testFilePath);
        assertTrue("8. Should receive a notification when changing a real file to the same real file with new content", observations.size()==5);

        File theOutputFile = new File(testFilePath);
        theOutputFile.delete();
        
        assertFalse("The deleted file should not exist", theOutputFile.exists()); 
        
        cm.setCertificateLocation(testFilePath);
        assertTrue("9. Should receive a notification when changing a real file to the same real filename that is now missing", observations.size()==6);

    }
    

}
