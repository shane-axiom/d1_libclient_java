package org.dataone.client.types;

import static org.junit.Assert.*;

import java.util.Date;

import org.dataone.service.types.v1.Identifier;
import org.junit.Before;
import org.junit.Test;

public class ObsoletesChainTest {

	private ObsoletesChain chain;
	private ObsoletesChain chainWithNulls;
	private Identifier foo1 = D1TypeBuilder.buildIdentifier("foo1");
	private Identifier foo2 = D1TypeBuilder.buildIdentifier("foo2");
	private Identifier foo3 = D1TypeBuilder.buildIdentifier("foo3");
	private Identifier foo4 = D1TypeBuilder.buildIdentifier("foo4");
	private Identifier foo5 = D1TypeBuilder.buildIdentifier("foo5");
	private Identifier foo6 = D1TypeBuilder.buildIdentifier("foo6");
	private Identifier foo7 = D1TypeBuilder.buildIdentifier("foo7");
	private Identifier foo8 = D1TypeBuilder.buildIdentifier("foo8");
	private Identifier foo9 = D1TypeBuilder.buildIdentifier("foo9");
	
	@Before
	public void setUp() throws Exception { 

		chain = new ObsoletesChain(foo3);
		chain.addObject(foo4, new Date(4000000), foo3, foo5, true);
		chain.addObject(foo3, new Date(3000000), foo2, foo4, true);
		chain.addObject(foo2, new Date(2000000), foo1, foo3, true);
		chain.addObject(foo1, new Date(1000000), null, foo2, true);
		
		chain.addObject(foo5, new Date(5000000), foo4, foo6, true);
		chain.addObject(foo6, new Date(6000000), foo5, foo7, true);
		chain.addObject(foo7, new Date(7000000), foo6, foo8, true);
		chain.addObject(foo8, new Date(8000000), foo7, foo9, true);
		chain.addObject(foo9, new Date(9000000), foo8, null, false);
		
		chainWithNulls = new ObsoletesChain(foo3);
		chainWithNulls.addObject(foo4, new Date(4000000), foo3, foo5, true);
		chainWithNulls.addObject(foo3, new Date(3000000), foo2, foo4, true);
		chainWithNulls.addObject(foo2, new Date(2000000), foo1, foo3, true);
		chainWithNulls.addObject(foo1, new Date(1000000), null, foo2, true);
		
		chainWithNulls.addObject(foo5, new Date(5000000), foo4, foo6, true);
		chainWithNulls.addObject(foo6, new Date(6000000), foo5, foo7, true);
		chainWithNulls.addObject(foo7, new Date(7000000), foo6, foo8, true);
		chainWithNulls.addObject(foo8, new Date(8000000), foo7, foo9, true);
		chainWithNulls.addObject(foo9, new Date(9000000), foo8, null, null);
		
		
	}

	@Test
	public void testGetStartingPoint() {
		assertTrue(chain.getStartingPoint().equals(foo3));
	}

//	@Test
	public void testAddObject() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetVersionAsOf() {
		assertTrue(chain.getVersionAsOf(new Date(6300000)).equals(foo6));
		assertNull(chain.getVersionAsOf(new Date(300000)));
		assertTrue(chain.getVersionAsOf(new Date(10300000)).equals(foo9));
	}

	@Test
	public void testNextVersion() {
		assertTrue(chain.nextVersion(foo2).equals(foo3));
		assertNull(chain.nextVersion(foo9));
	}

	@Test
	public void testPreviousVersion() {
		assertTrue(chain.previousVersion(foo8).equals(foo7));
		assertNull(chain.previousVersion(foo1));
	}

	@Test
	public void testGetLatestVersion() {
		assertTrue(chain.getLatestVersion().equals(foo9));
	}

	@Test
	public void testGetOriginalVersion() {
		assertTrue(chain.getOriginalVersion().equals(foo1));
	}

	@Test
	public void testGetByPosition() {
		assertTrue(chain.getByPosition(4).equals(foo5));
	}

	@Test
	public void testSize() {
		assertTrue(chain.size() == 9);
	}

	@Test
	public void testIsComplete() {
		assertTrue(chain.isComplete());
	}

	@Test
	public void testIsArchived() {
		assertTrue(chain.isArchived(foo2));	
		assertFalse(chain.isArchived(foo9));	
	}

	@Test
	public void testLatestIsArchived() {
		assertFalse(chain.latestIsArchived());
	}

	@Test
	public void testGetPublishDate() {
		Date expect = new Date(2000000);
		System.out.print(expect.getTime());
		System.out.print(chain.getPublishDate(foo2).getTime());
		assertTrue(chain.getPublishDate(foo2).equals(expect));
	}

	@Test
	public void testIsLatestVersion() {
		assertTrue(chain.isLatestVersion(foo9));
		assertFalse(chain.isLatestVersion(foo5));
	}
	
	
	@Test
	public void testIsArchived_Null() {
		assertTrue(chainWithNulls.isArchived(foo2));	
		assertFalse(chainWithNulls.isArchived(foo9));
	}
}
