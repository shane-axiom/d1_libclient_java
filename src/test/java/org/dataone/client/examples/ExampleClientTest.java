package org.dataone.client.examples;
/**
 * User: Andy Pippin
 * Date: 7/11/12
 */

import java.math.BigInteger;
import java.util.Date;

import org.dataone.client.v2.CNode;
import org.dataone.client.v2.itk.D1Client;
import org.dataone.service.exceptions.IdentifierNotUnique;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReplicationPolicy;
import org.dataone.service.types.v1.NodeState;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.Schedule;
import org.dataone.service.types.v1.Service;
import org.dataone.service.types.v1.Services;
import org.dataone.service.types.v1.Synchronization;
import org.dataone.service.types.v1.TypeFactory;
import org.dataone.service.types.v2.Node;
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
  
//  @Test
//  public void registerNode() throws ServiceFailure, NotImplemented, NotAuthorized, InvalidRequest, InvalidToken, IdentifierNotUnique {
//      CNode cn = D1Client.getCN("https://cn-dev-ucsb-1.test.dataone.org/cn");
//      Node n = new Node();
//      n.addContactSubject(TypeFactory.buildSubject("CN=Robert Nahf A579,O=Google,C=US,DC=cilogon,DC=org"));
//      n.addContactSubject(TypeFactory.buildSubject("CN=Mark Servilla A357,O=Google,C=US,DC=cilogon,DC=org"));
//      n.addSubject(TypeFactory.buildSubject("CN=urn:node:mnTestNCEI,DC=dataone,DC=org"));
//      n.setBaseURL("https://ncei-node.test.dataone.org/mn");
//      n.setDescription("DataONE Supported NCEI Slender Node Test Instance");
//      n.setIdentifier(TypeFactory.buildNodeReference("urn:node:mnTestNCEI"));
//      n.setName("DataONE Supported NCEI Slender Node Test Instance");
//      NodeReplicationPolicy nrp = new NodeReplicationPolicy();
//      nrp.setMaxObjectSize(new BigInteger("1073741824"));
//      nrp.setSpaceAllocated(new BigInteger("10995116277760"));
//      n.setNodeReplicationPolicy(nrp);
//      n.setReplicate(false);
//      n.setType(NodeType.MN);
//      n.setSynchronize(true);
//
//      Synchronization sync = new Synchronization();
//      Schedule sch = new Schedule();
//      sch.setHour("*");
//      sch.setMday("*");
//      sch.setMin("0/3");
//      sch.setMon("*");
//      sch.setSec("0");
//      sch.setWday("?");
//      sch.setYear("*");
//      sync.setSchedule(sch);
//      sync.setLastHarvested(new Date());
//      sync.setLastCompleteHarvest(new Date());
//      n.setSynchronization(sync);
//      
//      
//      Service s1 = new Service();
//      s1.setAvailable(true);
//      s1.setName("MNCore");
//      s1.setVersion("V1");
//      Service s2 = new Service();
//      s2.setAvailable(true);
//      s2.setName("MNRead");
//      s2.setVersion("V1");
//      
//      Services ss = new Services();
//      ss.addService(s1);
//      ss.addService(s2);
//      n.setServices(ss);
//      
//      n.setState(NodeState.UP);
//      
//      cn.register(null, n);
//      
//  }
}
