package org.dataone.configuration;

import static org.junit.Assert.assertEquals;

import org.junit.Test;


public class OverrideSettingsTest {
	
	/**
	 * Test lookup from original properties file in d1_common_java
	 */
	@Test
	public void testOriginal() {
		String foo = Settings.getConfiguration().getString("test.foo");
		assertEquals("default", foo);
	
	}
	
	/**
	 * Test looking up an overridden value in d1_libclient
	 * the resources on d1_libclient are loaded before the d1_common resources
	 * and therefore are considered to override them
	 */
	@Test
	public void testOverride() {
		String bar = Settings.getConfiguration().getString("test.bar");
		assertEquals("override", bar);
	
	}
}
