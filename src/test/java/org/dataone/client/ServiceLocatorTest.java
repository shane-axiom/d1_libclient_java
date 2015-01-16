package org.dataone.client;

import static org.junit.Assert.*;

import java.net.URI;

import org.dataone.client.v1.types.D1TypeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ServiceLocatorTest {

	org.dataone.client.v1.CNode v1CNode;
	org.dataone.service.cn.v1.CNCore v1CNCore;
	org.dataone.service.cn.v1.CNRead v1CNRead;
	org.dataone.client.v1.MNode v1MNode;
	
	D1ServiceLocator serviceLocator;
	
	
	@Before
	public void setUp() throws Exception {
		//serviceLocator;
	}

	@After
	public void tearDown() throws Exception {
	}

//	@Test
	public void shouldBeAbleToGetInstantiationsFromBaseUrl() {
		serviceLocator.getService(
				URI.create("https://cn-dev.test.dataone.org/cn"),
				org.dataone.client.v1.CNode.class, 
				null);
		serviceLocator.getService(
				URI.create("https://cn-dev.test.dataone.org/cn"),
				org.dataone.client.v2.CNode.class, 
				null);
		serviceLocator.getService(
				URI.create("https://cn-dev.test.dataone.org/cn"),
				org.dataone.client.v1.MNode.class, 
				null);
		serviceLocator.getService(
				URI.create("https://cn-dev.test.dataone.org/cn"),
				org.dataone.client.v2.MNode.class, 
				null);
	}

	
//	@Test
	public void shouldBeAbleToInstantiateFromNodeReference() {
		serviceLocator.getService(
				D1TypeBuilder.buildNodeReference("asdf"),
				org.dataone.client.v1.CNode.class, 
				null);
		serviceLocator.getService(
				D1TypeBuilder.buildNodeReference("asdf"),
				org.dataone.client.v2.CNode.class, 
				null);
		serviceLocator.getService(
				D1TypeBuilder.buildNodeReference("asdf"),
				org.dataone.client.v1.MNode.class, 
				null);
		serviceLocator.getService(
				D1TypeBuilder.buildNodeReference("asdf"),
				org.dataone.client.v2.MNode.class, 
				null);
	}

	
//	@Test
	public void shouldBeAbleToInstantiateFromInterface() {

		v1CNCore = serviceLocator.getService(
				URI.create("https://cn.dataone.org/cn/"), 
				org.dataone.service.cn.v1.CNCore.class, 
				null);
		v1CNRead = serviceLocator.getService(
				URI.create("https://cn.dataone.org/cn/"), 
				org.dataone.service.cn.v1.CNRead.class, 
				null);
		assertEquals("these should be the same instance of a v1 CNode", v1CNCore,v1CNRead);
	}
	
	
//	@Test
	public void shouldReturnTheSameInstanceForSameNodeVersion() {
		v1CNCore = serviceLocator.getService(
				URI.create("https://cn.dataone.org/cn/"), 
				org.dataone.service.cn.v1.CNCore.class, 
				null);
		v1CNRead = serviceLocator.getService(
				URI.create("https://cn.dataone.org/cn/"), 
				org.dataone.service.cn.v1.CNRead.class, 
				null);
		assertEquals("these should be the same instance of a v1 CNode", v1CNCore,v1CNRead);		
	}
	
	//TODO: should we be able to instantiate a test node by URI without it being
	//      in the nodeRegistry?  scenario would be: testing node registration
	// the difference is that in normal situations, a node is stood up independently.
	// but in this case instantiating the MNode, for example, is standing up the node.
//	@Test
	public void shouldThrowExceptionIfServiceNotValidatedAndReturningInterface() {
		D1ServiceLocator serviceLocator = null;
		v1MNode = serviceLocator.getService(
				URI.create("java:org.dataone.client.v1.impl.InMemoryMNode#Subject=mnAdmin&Subject=cnAdmin"), 
				org.dataone.client.v1.MNode.class, null);
	}
	
	@Test
	public void shouldWorkAcrossEnvironments() {
		
	}

	@Test
	public void shouldBeAbleToReturnNonStandardImplementations() {
		
	}
	
	
}
