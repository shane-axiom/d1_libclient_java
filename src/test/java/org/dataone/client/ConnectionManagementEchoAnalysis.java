/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */

package org.dataone.client;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;
import org.dataone.client.impl.rest.MultipartCNode;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeList;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.ReplicationStatus;
import org.junit.Ignore;
import org.junit.Test;

/**
 * a test case for examining socket leaks from httpClient (RestClient, HttpMultipartRestClient,
 * MultipartD1Node implementations, too).
 * 
 * see: http://stackoverflow.com/questions/4724193/how-can-i-ensure-that-my-httpclient-4-1-does-not-leak-sockets
 * see:  https://issues.apache.org/jira/browse/SOLR-861
 * see: https://forums.aws.amazon.com/thread.jspa?threadID=67603
 * see: http://stackoverflow.com/questions/4728683/workaround-to-not-shutdown-defaulthttpclient-each-time-after-usage
 * 
 * @author rnahf
 *
 */
public class ConnectionManagementEchoAnalysis {

	private static String echoNodeURL = "http://dev-testing.dataone.org/testsvc";
//	private static String echoResource = "echo";
//	private static String mmEchoResource = "echomm";
	

	/**
	 * test the easy case of sequentially repeated calls to a service.
	 * (Ran for 3hrs, making 367000 calls without throwing exceptions, 
	 * so this test probably doesn't capture the existing problem).
	 */
	@Ignore("this is a long-long-running test!")
	@Test
	public void testConnectionClose_singleThread_CLOSE_WAIT() 
	{
		MultipartCNode echoNode = new MultipartCNode(echoNodeURL + "/echo");
		
		int i = 0;
		try {
			for (i=0; i < 1000 * 1000; i++) {
				try {
					NodeList nl = echoNode.listNodes();
				} catch (ServiceFailure e) {
					if (!e.getDescription().contains("request.META"))
						throw e;
				}
				if (i % 100 == 0) {
					System.out.print(i + "\t");
					if (i % 1000 == 0) {
						System.out.println();
					}
				}
			}
		} catch (NotImplemented e) {
			System.out.println("NotImplemented exception at call " + i + " : " + e.getDescription());
		} catch (ServiceFailure e) {
			System.out.println("ServiceFailure exception at call " + i + " : " + e.getDescription());
		}
	}
	
	
	@Ignore("this is a long-long-running test!")
	@Test
	public void testConnectionClose_multiThread_CLOSE_WAIT() throws InterruptedException 
	{
		ExecutorService execService = Executors.newFixedThreadPool(16);
		
		int i = 0;
		try {
			for (i=0; i <= 10 * 1000; i++) {
				execService.execute(new Runnable() 
				{
					final MultipartCNode echoNode = new MultipartCNode(echoNodeURL + "/echo");
					final Identifier pid = new Identifier();
					final NodeReference nodeRef = new NodeReference();
			
					public void run() {
						try {
							System.out.print(".");
							pid.setValue("foobar");
							nodeRef.setValue("foobar");
							boolean x = echoNode.setReplicationStatus(null,
									pid, nodeRef,
									ReplicationStatus.COMPLETED,null);
						} catch (ServiceFailure e) {
							if (!e.getDescription().contains("request.META"))
								throw new RuntimeException(e.getDescription());
						} catch (BaseException e) {
							throw new RuntimeException(e.getDescription());
						}
					}
				});
				
				
				if (i % 100 == 0) {
					System.out.print(i + "\t");

					
					if (i % 1000 == 0) {
						System.out.println();
					}
				}
			}

			
	        // prevent other tasks from being added to the queue
	        execService.shutdown();
	        
	        String[] closeWaitCmd = {
	        		"/bin/sh",
	        		"-c",
	        		"netstat -a | grep CLOSE_WAIT | wc -l"
	        };
	        
	        String[] establishedCmd = {
	        		"/bin/sh",
	        		"-c",
	        		"netstat -a | grep ESTABLISHED | wc -l"
	        };
			
	        String[] streamCmd = {
	        		"/bin/sh",
	        		"-c",
	        		"netstat -a | grep stream | wc -l"
	        };
	        
	        String[] dgramCmd = {
	        		"/bin/sh",
	        		"-c",
	        		"netstat -a | grep dgram | wc -l"
	        };
	        for (;;) {
	        	
	        	try {
	        		// Run netstat
	        		System.out.printf("\ncw: %d est: %d stream:%d dgram:%d\n",
	        				netStatLineCount(closeWaitCmd),
	        				netStatLineCount(establishedCmd),
	        				netStatLineCount(streamCmd),
	        				netStatLineCount(dgramCmd));
	        	} catch (Exception e) {
	        		e.printStackTrace(System.err);
	        	}
	        	Thread.sleep(6*1000);
	        }
			
			
		} catch (RuntimeException e) {
			System.out.println("NotImplemented exception at call " + i + " : " + e.getMessage());
		}
	}
	
	private int netStatLineCount(String[] command) throws IOException {
		Process process = Runtime.getRuntime().exec(command);
		String wc = IOUtils.toString(process.getInputStream());
		int lineCount = Integer.valueOf(wc.trim());
		return lineCount;
	}
	
}
