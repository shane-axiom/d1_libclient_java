package org.dataone.client.examples;
/**
 * User: Andy Pippin
 * Date: 7/11/12
 */

import org.junit.Test;

/**
 * Allow the example to be run as a Maven test:
 *
 *   mvn -Dtest=ExampleClientTest test
 *
 */
public class ExampleClientTest {

  @Test
  public void runExample() throws Exception {
	  org.dataone.client.v1.examples.ExampleClient.main(new String[]{});
  }
}
