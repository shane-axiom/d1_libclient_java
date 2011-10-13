package org.dataone.ore;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dspace.foresite.Agent;
import org.dspace.foresite.AggregatedResource;
import org.dspace.foresite.Aggregation;
import org.dspace.foresite.OREException;
import org.dspace.foresite.OREFactory;
import org.dspace.foresite.OREParser;
import org.dspace.foresite.OREParserException;
import org.dspace.foresite.OREParserFactory;
import org.dspace.foresite.OREResource;
import org.dspace.foresite.ORESerialiser;
import org.dspace.foresite.ORESerialiserException;
import org.dspace.foresite.ORESerialiserFactory;
import org.dspace.foresite.Predicate;
import org.dspace.foresite.ResourceMap;
import org.dspace.foresite.ResourceMapDocument;
import org.dspace.foresite.Triple;
import org.dspace.foresite.TripleSelector;
import org.dspace.foresite.Vocab;
import org.dspace.foresite.jena.TripleJena;

public class ResourceMapFactory {
	
	// TODO: will this always resolve?
	private static final String D1_URI_PREFIX = Settings.getConfiguration().getString("D1Client.CN_URL") + "/object/";

	private static final String RESOURCE_MAP_SERIALIZATION_FORMAT = "RDF/XML";

	private static Predicate DC_TERMS_IDENTIFIER = null;
	
	private static Predicate CITO_IS_DOCUMENTED_BY = null;
	
	private static Predicate CITO_DOCUMENTS = null;

	private static ResourceMapFactory instance = null;
	
	private void init() throws URISyntaxException {
		// use as much as we can from the included Vocab for dcterms:Agent
		DC_TERMS_IDENTIFIER = new Predicate();
		DC_TERMS_IDENTIFIER.setNamespace(Vocab.dcterms_Agent.ns().toString());
		DC_TERMS_IDENTIFIER.setPrefix(Vocab.dcterms_Agent.schema());
		DC_TERMS_IDENTIFIER.setName("identitifer");
		DC_TERMS_IDENTIFIER.setURI(new URI(DC_TERMS_IDENTIFIER.getNamespace() + DC_TERMS_IDENTIFIER.getName()));
		
		// create the CITO:isDocumentedBy predicate
		CITO_IS_DOCUMENTED_BY = new Predicate();
		CITO_IS_DOCUMENTED_BY.setNamespace("http://purl.org/spar/cito/");
		CITO_IS_DOCUMENTED_BY.setPrefix("cito");
		CITO_IS_DOCUMENTED_BY.setName("isDocumentedBy");
		CITO_IS_DOCUMENTED_BY.setURI(new URI(CITO_IS_DOCUMENTED_BY.getNamespace() + CITO_IS_DOCUMENTED_BY.getName()));
		
		// create the CITO:documents predicate
		CITO_DOCUMENTS = new Predicate();
		CITO_DOCUMENTS.setNamespace(CITO_IS_DOCUMENTED_BY.getNamespace());
		CITO_DOCUMENTS.setPrefix(CITO_IS_DOCUMENTED_BY.getPrefix());
		CITO_DOCUMENTS.setName("documents");
		CITO_DOCUMENTS.setURI(new URI(CITO_DOCUMENTS.getNamespace() + CITO_DOCUMENTS.getName()));
	}
	
	private ResourceMapFactory() {
		try {
			init();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static ResourceMapFactory getInstance() {
		if (instance == null) {
			instance = new ResourceMapFactory();
		}
		return instance;
	}
	
	public ResourceMap createResourceMap(
			Identifier resourceMapId, 
			Map<Identifier, List<Identifier>> idMap) 
		throws OREException, URISyntaxException {
		
		// create the resource map and the aggregation
		// FIXME: more appropriate aggregation id
		Aggregation aggregation = OREFactory.createAggregation(new URI("ore://d1.aggregation"));
		ResourceMap resourceMap = aggregation.createResourceMap(new URI(D1_URI_PREFIX + resourceMapId.getValue()));
		
		Agent creator = OREFactory.createAgent();
		creator.addName("Java libclient");
		resourceMap.addCreator(creator);
		//aggregation.addCreator(creator);
		aggregation.addTitle("DataONE Aggregation");
		
		for (Identifier metadataId: idMap.keySet()) {
		
			// add the science metadata
			AggregatedResource metadataResource = aggregation.createAggregatedResource(new URI(D1_URI_PREFIX + metadataId.getValue()));
			Triple metadataIdentifier = new TripleJena();
			metadataIdentifier.initialise(metadataResource);
			metadataIdentifier.relate(DC_TERMS_IDENTIFIER, metadataId.getValue());
			resourceMap.addTriple(metadataIdentifier);
			aggregation.addAggregatedResource(metadataResource);
	
			// iterate through data
			List<Identifier> dataIds = idMap.get(metadataId);
			for (Identifier dataId: dataIds) {
				AggregatedResource dataResource = aggregation.createAggregatedResource(new URI(D1_URI_PREFIX + dataId.getValue()));
				// dcterms:identifier
				Triple identifier = new TripleJena();
				identifier.initialise(dataResource);
				identifier.relate(DC_TERMS_IDENTIFIER, dataId.getValue());
				resourceMap.addTriple(identifier);
				// cito:isDocumentedBy
				Triple isDocumentedBy = new TripleJena();
				isDocumentedBy.initialise(dataResource);
				isDocumentedBy.relate(CITO_IS_DOCUMENTED_BY, metadataResource);
				resourceMap.addTriple(isDocumentedBy);
				// cito:documents (on metadata resource)
				Triple documents = new TripleJena();
				documents.initialise(metadataResource);
				documents.relate(CITO_DOCUMENTS, dataResource);
				resourceMap.addTriple(documents);
				
				aggregation.addAggregatedResource(dataResource);
			}
		}
		
		return resourceMap;
		
	}
	
	public Map<Identifier, List<Identifier>> parseResourceMap(String resourceMapContents) 
		throws OREException, URISyntaxException, UnsupportedEncodingException, OREParserException {
		Map<Identifier, List<Identifier>> idMap = new HashMap<Identifier, List<Identifier>>();
		
		OREParser parser = OREParserFactory.getInstance(RESOURCE_MAP_SERIALIZATION_FORMAT);
		InputStream is = new ByteArrayInputStream(resourceMapContents.getBytes("UTF-8"));
		ResourceMap resourceMap = parser.parse(is);
		Aggregation aggregation = resourceMap.getAggregation();
		List<AggregatedResource> resources = aggregation.getAggregatedResources();
		for (AggregatedResource entry: resources) {
			// metadata entries should have everything we need to reconstruct the model
			Identifier metadataId = new Identifier();
			List<Identifier> dataIds = new ArrayList<Identifier>();
			
			TripleSelector documentsSelector = new TripleSelector(null, CITO_DOCUMENTS.getURI(), null);
			List<Triple> documentsTriples = entry.listTriples(documentsSelector);
			if (documentsTriples.isEmpty()) {
				continue;
			}
			
			// get the identifier of the metadata
			TripleSelector identifierSelector = new TripleSelector(null, DC_TERMS_IDENTIFIER.getURI(), null);
			List<Triple> identifierTriples = entry.listTriples(identifierSelector);
			if (!identifierTriples.isEmpty()) {
				String metadataIdValue = identifierTriples.get(0).getObjectLiteral();
				metadataId.setValue(metadataIdValue);
			}
			
			// iterate through the data entries
			for (Triple triple: documentsTriples) {
				String dataIdValue = null;
				// try getting it from the model
				OREResource dataResource = triple.getObject();
				if (dataResource != null) {
					dataIdValue = dataResource.listTriples(identifierSelector).get(0).getObjectLiteral();
				}
				// FIXME: otherwise we use URI parsing, bleck!
				if (dataIdValue == null) {
					// get the dataIds we are documenting
					String dataResourceURI = triple.getObjectURI().toString();
					dataIdValue = dataResourceURI.substring(D1_URI_PREFIX.length());
				}
				Identifier dataId = new Identifier();
				dataId.setValue(dataIdValue);
				dataIds.add(dataId);
			}
			
			// add the entry
			idMap.put(metadataId , dataIds);
			
		}
		
		return idMap;
		
	}
	
	public String serializeResourceMap(ResourceMap resourceMap) throws ORESerialiserException {
		// serialize
		ORESerialiser serializer = ORESerialiserFactory.getInstance(RESOURCE_MAP_SERIALIZATION_FORMAT);
		ResourceMapDocument doc = serializer.serialise(resourceMap);
		String serialisation = doc.toString();
		return serialisation;
	}

}
