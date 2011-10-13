package org.dataone.ore;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dataone.service.types.v1.Identifier;
import org.dspace.foresite.ResourceMap;
import org.junit.Test;

public class ResourceMapFactoryTest {
	
	
	@Test
	public void testCreateResourceMap() {
		
		try {
			Identifier resourceMapId = new Identifier();
			resourceMapId.setValue("map.1.1");
			Identifier metadataId = new Identifier();
			metadataId.setValue("meta.1.1");
			List<Identifier> dataIds = new ArrayList<Identifier>();
			Identifier dataId = new Identifier();
			dataId.setValue("data.1.1");
			Identifier dataId2 = new Identifier();
			dataId2.setValue("data.2.1");
			dataIds.add(dataId);
			dataIds.add(dataId2);
			Map<Identifier, List<Identifier>> idMap = new HashMap<Identifier, List<Identifier>>();
			idMap.put(metadataId, dataIds);
			ResourceMap resourceMap = ResourceMapFactory.getInstance().createResourceMap(resourceMapId, idMap);
			assertNotNull(resourceMap);
			String rdfXml = ResourceMapFactory.getInstance().serializeResourceMap(resourceMap);
			assertNotNull(rdfXml);
			System.out.println(rdfXml);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
}
