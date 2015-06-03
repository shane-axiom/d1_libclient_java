package org.dataone.client.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.dataone.client.D1Node;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.rest.HttpMultipartRestClient;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.client.v1.CNode;
import org.dataone.client.v1.MNode;
import org.dataone.client.v1.impl.InMemoryMNode;
import org.dataone.client.v1.impl.MultipartCNode;
import org.dataone.client.v1.impl.NodeListNodeLocator;
import org.dataone.client.v1.types.D1TypeBuilder;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeList;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class V1NodeListNodeLocatorTest {

	private NodeList nodeList;
	private NodeListNodeLocator nodeLoc;
	private MultipartRestClient mrc;

	private static Node buildNode(String baseUri, NodeType type, String nodeID) {
		Node n = new Node();
		n.setType(type);
		n.setIdentifier(D1TypeBuilder.buildNodeReference(nodeID));
		n.setBaseURL(baseUri);
		return n;
	}
	
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {

		nodeList = new NodeList();
		
		nodeList.addNode(buildNode("https://cn1.foo.org/cn", NodeType.CN, "urn:node:CN1foo"));
		nodeList.addNode(buildNode("https://cn2.bar.org/cn", NodeType.CN, "urn:node:CN1bar"));
		nodeList.addNode(buildNode("https://mn1.biz.org/mn", NodeType.MN, "urn:node:MNhttps"));
		nodeList.addNode(buildNode("http://mn2.baz.org/knb/d1/mn", NodeType.MN, "urn:node:MNhttp"));
		nodeList.addNode(buildNode("java:org.dataone.client.v1.impl.InMemoryMNode#Subject=mnAdmin&Subject=cnAdmin,", 
				 NodeType.MN, "urn:node:MNjava"));
		mrc = new HttpMultipartRestClient();
		
		nodeLoc = new NodeListNodeLocator(nodeList, mrc);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testNodeListNodeLocator_parameterizedConstructor() {
		
		assertTrue("NodeLocator should be instantiated", nodeLoc != null);
		
	}
	
	@Test
	public void testGetMNode_https() throws ClientSideException {
		D1Node mn = nodeLoc.getNode(D1TypeBuilder.buildNodeReference("urn:node:MNhttps"));
	}
	
	@Test
	public void testGetMNode_http() throws ClientSideException {
		D1Node mn = nodeLoc.getNode(D1TypeBuilder.buildNodeReference("urn:node:MNhttp"));	
	}
	
	@Test
	public void testGetMNode_javaClass() throws ClientSideException {
		D1Node mn = nodeLoc.getNode(D1TypeBuilder.buildNodeReference("urn:node:MNjava"));	
	}

	@Test
	public void testGetCNode_byNodeReference() throws ClientSideException {
		D1Node cn = nodeLoc.getNode(D1TypeBuilder.buildNodeReference("urn:node:CN1foo"));
	}

	@Test
	public void testGetCNode() throws ClientSideException {
		D1Node cn = nodeLoc.getCNode();
	}

	@Test
	public void testPutMNode() throws ClientSideException {
		NodeReference node = D1TypeBuilder.buildNodeReference("beep");
		MNode mn = new InMemoryMNode(
						D1TypeBuilder.buildSubject("admin1"),
						D1TypeBuilder.buildSubject("admin2")
						);
		
		nodeLoc.putNode(node,mn);
		D1Node x = nodeLoc.getNode(node);
		assertTrue("Got MNode through NodeLocator", mn.equals(x));
		assertEquals("Should now have 6 Nodes registered", 6, nodeLoc.listD1Nodes().size());
		assertEquals("Should now have 4 MNodes registered", 4, nodeLoc.listD1Nodes(NodeType.MN).size());
	}

	@Test
	public void testPutCNode() throws ClientSideException {
		NodeReference node = D1TypeBuilder.buildNodeReference("beep");
		
		CNode cn = new MultipartCNode(mrc, "someBaseUrl");
		
		nodeLoc.putNode(node,cn);
		D1Node x = nodeLoc.getNode(node);
		assertTrue("Got MNode through NodeLocator", cn.equals(x));
		assertEquals("Should now have 6 Nodes registered", 6, nodeLoc.listD1Nodes().size());
		assertEquals("Should now have 3 MNodes registered", 3, nodeLoc.listD1Nodes(NodeType.CN).size());

	}

	@Test
	public void testListD1Nodes() {
		Set<NodeReference> nodes = nodeLoc.listD1Nodes();
		assertEquals("Should have 5 nodes in total", 5, nodes.size());
	}

	@Test
	public void testListD1NodesNodeType() {
		Set<NodeReference> nodes = nodeLoc.listD1Nodes(NodeType.CN);
		assertEquals("Should have 2 CNs in total", 2, nodes.size());
		
		nodes = nodeLoc.listD1Nodes(NodeType.MN);
		assertEquals("Should have 3 MNs in total", 3, nodes.size());
	}
	
	@Test
	public void testNodeIdIsSet() throws ClientSideException {
	    D1Node dn = nodeLoc.getNode(D1TypeBuilder.buildNodeReference("urn:node:CN1foo"));
	    assertTrue("returned D1Node's NodeId should not be null when it is contained in the Node record.",
	            dn.getNodeId() != null);
	    
	}
	
	@Test
    public void testGetCNodeIdIsSet() throws ClientSideException {
        CNode cn = nodeLoc.getCNode();
        assertTrue("returned CNode's NodeId should not be null when it is contained in the Node record.",
                cn.getNodeId() != null);
        
    }

}
