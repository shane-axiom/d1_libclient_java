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

package org.dataone.ore;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.D1TypeBuilder;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.util.EncodingUtilities;
import org.dspace.foresite.Agent;
import org.dspace.foresite.AggregatedResource;
import org.dspace.foresite.Aggregation;
import org.dspace.foresite.OREException;
import org.dspace.foresite.OREFactory;
import org.dspace.foresite.OREParser;
import org.dspace.foresite.OREParserException;
import org.dspace.foresite.OREParserFactory;
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


/**
 *  A facility for serializing and deserializing Resource Maps used by DataONE,
 *  following the conventions and constraints detailed in 
 *  http://mule1.dataone.org/ArchitectureDocs-current/design/DataPackage.html
 *  
 *  Note: DataONE uses the serialized form of ResourceMaps to persist the relationships
 *  between metadata objects and the data objects they document, and builds a
 *  ResourceMap as an intermediate form to derive the relationships into a simpler
 *  "identifier map" representation.
 *
 */
public class ResourceMapFactory {
	
	// TODO: will this always resolve?
	private static final String D1_URI_PREFIX = Settings.getConfiguration()
			.getString("D1Client.CN_URL","howdy") + "/v1/resolve/";

	private static final String RESOURCE_MAP_SERIALIZATION_FORMAT = "RDF/XML";

	private static Predicate DC_TERMS_IDENTIFIER = null;
	
	private static Predicate CITO_IS_DOCUMENTED_BY = null;
	
	private static Predicate CITO_DOCUMENTS = null;

	private static ResourceMapFactory instance = null;
	
	private static Log log = LogFactory.getLog(ResourceMapFactory.class);
	
	private void init() throws URISyntaxException {
		// use as much as we can from the included Vocab for dcterms:Agent
		DC_TERMS_IDENTIFIER = new Predicate();
		DC_TERMS_IDENTIFIER.setNamespace(Vocab.dcterms_Agent.ns().toString());
		DC_TERMS_IDENTIFIER.setPrefix(Vocab.dcterms_Agent.schema());
		DC_TERMS_IDENTIFIER.setName("identifier");
		DC_TERMS_IDENTIFIER.setURI(new URI(DC_TERMS_IDENTIFIER.getNamespace() 
				+ DC_TERMS_IDENTIFIER.getName()));
		
		// create the CITO:isDocumentedBy predicate
		CITO_IS_DOCUMENTED_BY = new Predicate();
		CITO_IS_DOCUMENTED_BY.setNamespace("http://purl.org/spar/cito/");
		CITO_IS_DOCUMENTED_BY.setPrefix("cito");
		CITO_IS_DOCUMENTED_BY.setName("isDocumentedBy");
		CITO_IS_DOCUMENTED_BY.setURI(new URI(CITO_IS_DOCUMENTED_BY.getNamespace() 
				+ CITO_IS_DOCUMENTED_BY.getName()));
		
		// create the CITO:documents predicate
		CITO_DOCUMENTS = new Predicate();
		CITO_DOCUMENTS.setNamespace(CITO_IS_DOCUMENTED_BY.getNamespace());
		CITO_DOCUMENTS.setPrefix(CITO_IS_DOCUMENTED_BY.getPrefix());
		CITO_DOCUMENTS.setName("documents");
		CITO_DOCUMENTS.setURI(new URI(CITO_DOCUMENTS.getNamespace() 
				+ CITO_DOCUMENTS.getName()));
	}
	
	private ResourceMapFactory() {
		try {
			init();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Returns the singleton instance for this class.
	 * @return
	 */
	public static ResourceMapFactory getInstance() {
		if (instance == null) {
			instance = new ResourceMapFactory();
		}
		return instance;
	}

	
	/**
	 * creates a ResourceMap from the DataPackage representation.
	 * @param resourceMapId
	 * @param idMap
	 * @return
	 * @throws OREException
	 * @throws URISyntaxException
	 */
	public ResourceMap createResourceMap(
			Identifier resourceMapId, 
			Map<Identifier, List<Identifier>> idMap) 
		throws OREException, URISyntaxException {
		
		// create the resource map and the aggregation
		// NOTE: use distinct, but related URI for the aggregation
		Aggregation aggregation = OREFactory.createAggregation(new URI(D1_URI_PREFIX 
				+ EncodingUtilities.encodeUrlPathSegment(resourceMapId.getValue()) 
				+ "#aggregation"));
		ResourceMap resourceMap = aggregation.createResourceMap(new URI(D1_URI_PREFIX 
				+ EncodingUtilities.encodeUrlPathSegment(resourceMapId.getValue())));
		
		Agent creator = OREFactory.createAgent();
		creator.addName("Java libclient");
		resourceMap.addCreator(creator);
		// add the resource map identifier
		Triple resourceMapIdentifier = new TripleJena();
		resourceMapIdentifier.initialise(resourceMap);
		resourceMapIdentifier.relate(DC_TERMS_IDENTIFIER, resourceMapId.getValue());
		resourceMap.addTriple(resourceMapIdentifier);
		
		//aggregation.addCreator(creator);
		aggregation.addTitle("DataONE Aggregation");
		
		// iterate through the metadata items
		for (Identifier metadataId: idMap.keySet()) {
		
			// add the science metadata
			AggregatedResource metadataResource = aggregation.createAggregatedResource(new URI(D1_URI_PREFIX 
					+ EncodingUtilities.encodeUrlPathSegment(metadataId.getValue())));
			Triple metadataIdentifier = new TripleJena();
			metadataIdentifier.initialise(metadataResource);
			metadataIdentifier.relate(DC_TERMS_IDENTIFIER, metadataId.getValue());
			resourceMap.addTriple(metadataIdentifier);
			aggregation.addAggregatedResource(metadataResource);
	
			// iterate through data items
			List<Identifier> dataIds = idMap.get(metadataId);
			for (Identifier dataId: dataIds) {
				AggregatedResource dataResource = aggregation.createAggregatedResource(new URI(D1_URI_PREFIX 
						+ EncodingUtilities.encodeUrlPathSegment(dataId.getValue())));
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
	
	
	/**
	 * Parses the string containing serialized form into the Map representation
     * used by the org.dataone.client.DataPackage class.
	 * @param resourceMapContents
	 * @return
	 * @throws OREException
	 * @throws URISyntaxException
	 * @throws UnsupportedEncodingException
	 * @throws OREParserException
	 */
	public Map<Identifier, Map<Identifier, List<Identifier>>> parseResourceMap(String resourceMapContents) 
	throws OREException, URISyntaxException, UnsupportedEncodingException, OREParserException {
		InputStream is = new ByteArrayInputStream(resourceMapContents.getBytes("UTF-8"));
		return parseResourceMap(is);
	}

	
    /**
     * Parses the input stream from the serialized form into the Map representation
     * used by the org.dataone.client.DataPackage class.
     * @param is
     * @return
     * @throws OREException - also thrown when identifier triple is missing from 
     *         any aggregated resource.
     * @throws URISyntaxException
     * @throws UnsupportedEncodingException
     * @throws OREParserException
     */
	public Map<Identifier, Map<Identifier, List<Identifier>>> parseResourceMap(InputStream is) 
	throws OREException, URISyntaxException, UnsupportedEncodingException, OREParserException 
	{
		// the inner map of the return object	
		Map<Identifier, List<Identifier>> idMap = new HashMap<Identifier, List<Identifier>>();
			
		OREParser parser = OREParserFactory.getInstance(RESOURCE_MAP_SERIALIZATION_FORMAT);
		ResourceMap resourceMap = parser.parse(is);
		if (resourceMap == null) {
			throw new OREException("Null resource map returned from OREParser (format: " + 
					RESOURCE_MAP_SERIALIZATION_FORMAT + ")");
		}
		
		TripleSelector idSelector = new TripleSelector(null, DC_TERMS_IDENTIFIER.getURI(), null);
		TripleSelector documentsSelector = new TripleSelector(null, CITO_DOCUMENTS.getURI(), null);
		TripleSelector isDocBySelector = new TripleSelector(null, CITO_IS_DOCUMENTED_BY.getURI(), null);
		
		
		
		// get the identifier of the whole package ResourceMap
		Identifier packageId = new Identifier();
		log.debug(resourceMap.getURI());
	
		
		// get the resource map identifier
		List<Triple> packageIdTriples = resourceMap.listTriples(idSelector);
		if (!packageIdTriples.isEmpty()) {
			packageId.setValue(packageIdTriples.get(0).getObjectLiteral());
		} else {
			throw new OREException("No Identifer statement was found for the " +
					"resourceMap resource ('" + resourceMap.getURI().toString() + "')");
		}
	
		// Process the aggregated resources to get the other relevant statements
		List<AggregatedResource> resources = resourceMap.getAggregation().getAggregatedResources();

		// assemble an identifier map from the aggregated resources first, to
		// make life easier later
		HashMap<String, Identifier> idHash = new HashMap<String,Identifier>();
		for (AggregatedResource ar: resources) {
			List<Triple> idTriples = ar.listTriples(idSelector);
			if (!idTriples.isEmpty()) {  // need an identifier to do anything
				Identifier arId = D1TypeBuilder.buildIdentifier(idTriples.get(0).getObjectLiteral());
				idHash.put(ar.getURI().toString(), arId);
			} else {
				throw new OREException("Aggregated resource '" + ar.getURI().toString() + 
						"' in the resource map is missing the required Identifier statement");
			}
		}
		
		HashMap<String, Set<String>> metadataMap = new HashMap<String, Set<String>>();
		for (AggregatedResource ar: resources) {
			log.debug("Agg resource: " + ar.getURI());

			List<Triple> documentsTriples = ar.listTriples(documentsSelector);
			log.debug("--- documents count: " + documentsTriples.size());
			
			if (!documentsTriples.isEmpty()) {
				// get all of the objects this resource documents
				String metadataURI = ar.getURI().toString();
				log.debug("  ---metadataURI : "  + metadataURI);
				if (!metadataMap.containsKey(metadataURI))  {
					metadataMap.put(metadataURI, new HashSet<String>());
					log.debug("Creating new HashSet for: " + metadataURI + " : " + metadataMap.get(metadataURI).size());
				}
				for (Triple trip : documentsTriples) {
					String documentsObject = trip.getObjectURI().toString();
					log.debug("  ---documentsObject: " + documentsObject);
					metadataMap.get(metadataURI).add(documentsObject);
				}
			}
			
			
			List<Triple> docByTriples = ar.listTriples(isDocBySelector);
			log.debug("+++ isDocBy count: " + docByTriples.size());
			if (!docByTriples.isEmpty()) {
				// get all of the objects this resource is documented by
				String docBySubjectURI = ar.getURI().toString();
				log.debug("  +++docBySubjectURI: " + docBySubjectURI);
				for (Triple trip : docByTriples) {
					String metadataURI = trip.getObjectURI().toString();
					log.debug("  +++metadataURI: " + metadataURI);
					if (!metadataMap.containsKey(metadataURI)) {
						metadataMap.put(metadataURI,new HashSet<String>());
						log.debug("Creating new HashSet for: " + metadataURI + " : " + metadataMap.get(metadataURI).size());
					}
					metadataMap.get(metadataURI).add(docBySubjectURI);
				}
			}
		}
	
		for (String metadata : metadataMap.keySet()) {
			Identifier metadataID = idHash.get(metadata);
			List<Identifier> dataIDs = new ArrayList<Identifier>();
			log.debug("~~~~~ data count: " + metadata + ": " + metadataMap.get(metadata).size());
			for (String dataURI: metadataMap.get(metadata)) {
				Identifier pid = idHash.get(dataURI);
				dataIDs.add(pid);
			}
			idMap.put(metadataID, dataIDs);
		}

		// Now group the packageId with the Map of metadata/data Ids and return it
		Map<Identifier, Map<Identifier, List<Identifier>>> packageMap = 
				new HashMap<Identifier, Map<Identifier, List<Identifier>>>();
		packageMap.put(packageId, idMap);

		return packageMap;			
	}

	
	/**
	 * Serialize the ResourceMap
	 * @param resourceMap
	 * @return
	 * @throws ORESerialiserException
	 */
	public String serializeResourceMap(ResourceMap resourceMap) throws ORESerialiserException {
		// serialize
		ORESerialiser serializer = ORESerialiserFactory.getInstance(RESOURCE_MAP_SERIALIZATION_FORMAT);
		ResourceMapDocument doc = serializer.serialise(resourceMap);
		String serialisation = doc.toString();
		return serialisation;
	}

}
