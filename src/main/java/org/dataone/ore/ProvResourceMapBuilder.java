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
 * $Id: ResourceMapFactory.java 13886 2014-05-28 20:26:50Z rnahf $
 */

package org.dataone.ore;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.util.EncodingUtilities;
import org.dspace.foresite.Agent;
import org.dspace.foresite.AggregatedResource;
import org.dspace.foresite.Aggregation;
import org.dspace.foresite.OREException;
import org.dspace.foresite.OREFactory;
import org.dspace.foresite.Predicate;
import org.dspace.foresite.ResourceMap;
import org.dspace.foresite.Triple;
import org.dspace.foresite.Vocab;
import org.dspace.foresite.jena.TripleJena;

import com.hp.hpl.jena.rdf.model.Model;
/**
 *  A Resource Map builder with methods for adding provenance or other statements about
 *  resource in an ORE aggregation.  
 *  
 */
public class ProvResourceMapBuilder {
	
	// TODO: will this always resolve?
	private static final String D1_URI_PREFIX = Settings.getConfiguration()
			.getString("D1Client.CN_URL","howdy") + "/v1/resolve/";

	private static final String RESOURCE_MAP_SERIALIZATION_FORMAT = "RDF/XML";

	private static Predicate DC_TERMS_IDENTIFIER = null;
	
	private static Predicate CITO_IS_DOCUMENTED_BY = null;
	
	private static Predicate CITO_DOCUMENTS = null;
	
	private static Predicate PROV_WAS_DERIVED_FROM = null;
	
	private static Predicate PROV_WAS_GENERATED_BY = null;
	
	private static Predicate PROV_WAS_INFORMED_BY = null;
	
	private static Predicate PROV_USED = null;

	private static Predicate PROV_WAS_ASSOCIATED_WITH = null;
	
	private static Predicate PROV_QUALIFIED_ASSOCIATION = null;

	private static Predicate PROV_P_AGENT = null;

	private static Predicate PROV_HAD_PLAN = null;

	private static List<Predicate> predicates = null;

	private static ProvResourceMapBuilder instance = null;
	
	private static Model oreModel = null;
	
	private static Log log = LogFactory.getLog(ProvResourceMapBuilder.class);
	
	private static final String CITO_NAMESPACE_URI = "http://purl.org/spar/cito/";
	
	private static final String PROV_NAMESPACE_URI = "http://www.w3.org/ns/prov#";

	private void init() throws URISyntaxException {
		predicates = new ArrayList<Predicate>();
		
		// use as much as we can from the included Vocab for dcterms:Agent
		DC_TERMS_IDENTIFIER = new Predicate();
		DC_TERMS_IDENTIFIER.setNamespace(Vocab.dcterms_Agent.ns().toString());
		DC_TERMS_IDENTIFIER.setPrefix(Vocab.dcterms_Agent.schema());
		DC_TERMS_IDENTIFIER.setName("identifier");
		DC_TERMS_IDENTIFIER.setURI(new URI(DC_TERMS_IDENTIFIER.getNamespace() 
				+ DC_TERMS_IDENTIFIER.getName()));
		
		// create the CITO:isDocumentedBy predicate
		CITO_IS_DOCUMENTED_BY = new Predicate();
		CITO_IS_DOCUMENTED_BY.setNamespace(CITO_NAMESPACE_URI);
		CITO_IS_DOCUMENTED_BY.setPrefix("cito");
		CITO_IS_DOCUMENTED_BY.setName("isDocumentedBy");
		CITO_IS_DOCUMENTED_BY.setURI(new URI(CITO_NAMESPACE_URI 
				+ CITO_IS_DOCUMENTED_BY.getName()));
		
		// create the CITO:documents predicate
		CITO_DOCUMENTS = new Predicate();
		CITO_DOCUMENTS.setNamespace(CITO_IS_DOCUMENTED_BY.getNamespace());
		CITO_DOCUMENTS.setPrefix(CITO_IS_DOCUMENTED_BY.getPrefix());
		CITO_DOCUMENTS.setName("documents");
		CITO_DOCUMENTS.setURI(new URI(CITO_NAMESPACE_URI 
				+ CITO_DOCUMENTS.getName()));
		
		// create the PROV:wasDerivedFrom predicate
		PROV_WAS_DERIVED_FROM = new Predicate();
		PROV_WAS_DERIVED_FROM.setNamespace(PROV_NAMESPACE_URI);
		PROV_WAS_DERIVED_FROM.setPrefix("prov");
		PROV_WAS_DERIVED_FROM.setName("wasDerivedFrom");
		PROV_WAS_DERIVED_FROM.setURI(new URI(PROV_NAMESPACE_URI 
						+ PROV_WAS_DERIVED_FROM.getName()));
		
		// create the PROV:wasGeneratedBy predicate
		PROV_WAS_GENERATED_BY = new Predicate();
		PROV_WAS_GENERATED_BY.setNamespace(PROV_NAMESPACE_URI);
		PROV_WAS_GENERATED_BY.setPrefix(PROV_WAS_DERIVED_FROM.getPrefix());
		PROV_WAS_GENERATED_BY.setName("wasGeneratedBy");
		PROV_WAS_GENERATED_BY.setURI(new URI(PROV_NAMESPACE_URI 
						+ PROV_WAS_GENERATED_BY.getName()));
		
		// create the PROV:wasInformedBy predicate
		PROV_WAS_INFORMED_BY = new Predicate();
		PROV_WAS_INFORMED_BY.setNamespace(PROV_NAMESPACE_URI);
		PROV_WAS_INFORMED_BY.setPrefix(PROV_WAS_DERIVED_FROM.getPrefix());
		PROV_WAS_INFORMED_BY.setName("wasInformedBy");
		PROV_WAS_INFORMED_BY.setURI(new URI(PROV_NAMESPACE_URI 
						+ PROV_WAS_INFORMED_BY.getName()));
		
		// create the PROV:used predicate
		PROV_USED = new Predicate();
		PROV_USED.setNamespace(PROV_NAMESPACE_URI);
		PROV_USED.setPrefix(PROV_WAS_DERIVED_FROM.getPrefix());
		PROV_USED.setName("used");
		PROV_USED.setURI(new URI(PROV_NAMESPACE_URI + PROV_USED.getName()));
		
		// create the PROV:wasAssociatedWith predicate
		PROV_WAS_ASSOCIATED_WITH = new Predicate();
		PROV_WAS_ASSOCIATED_WITH.setNamespace(PROV_NAMESPACE_URI);
		PROV_WAS_ASSOCIATED_WITH.setPrefix(PROV_USED.getPrefix());
		PROV_WAS_ASSOCIATED_WITH.setName("wasAssociatedWith");
		PROV_WAS_ASSOCIATED_WITH.setURI(new URI(PROV_NAMESPACE_URI + 
				PROV_WAS_ASSOCIATED_WITH.getName()));
		
		// create the PROV:qualifiedAssociation predicate
		PROV_QUALIFIED_ASSOCIATION = new Predicate();
		PROV_QUALIFIED_ASSOCIATION.setNamespace(PROV_NAMESPACE_URI);
		PROV_QUALIFIED_ASSOCIATION.setPrefix(PROV_WAS_ASSOCIATED_WITH.getPrefix());
		PROV_QUALIFIED_ASSOCIATION.setName("qualifiedAssociation");
		PROV_QUALIFIED_ASSOCIATION.setURI(new URI(PROV_NAMESPACE_URI + 
				PROV_QUALIFIED_ASSOCIATION.getName()));
		
		// create the PROV:agent predicate
		PROV_P_AGENT = new Predicate();
		PROV_P_AGENT.setNamespace(PROV_NAMESPACE_URI);
		PROV_P_AGENT.setPrefix(PROV_QUALIFIED_ASSOCIATION.getPrefix());
		PROV_P_AGENT.setName("agent");
		PROV_P_AGENT.setURI(new URI(PROV_NAMESPACE_URI + 
				PROV_P_AGENT.getName()));
		
		// create the PROV:hadPlan predicate
		PROV_HAD_PLAN = new Predicate();
		PROV_HAD_PLAN.setNamespace(PROV_NAMESPACE_URI);
		PROV_HAD_PLAN.setPrefix(PROV_P_AGENT.getPrefix());
		PROV_HAD_PLAN.setName("hadPlan");
		PROV_HAD_PLAN.setURI(new URI(PROV_NAMESPACE_URI + 
				PROV_HAD_PLAN.getName()));
		
		// include predicates from each namespace we want to support
		predicates.add(CITO_DOCUMENTS);
		predicates.add(PROV_WAS_DERIVED_FROM);
		predicates.add(PROV_WAS_GENERATED_BY);
		predicates.add(PROV_WAS_INFORMED_BY);
		predicates.add(PROV_USED);
		predicates.add(PROV_WAS_ASSOCIATED_WITH);
		predicates.add(PROV_QUALIFIED_ASSOCIATION);
		predicates.add(PROV_P_AGENT);
		predicates.add(PROV_HAD_PLAN);
	}
	
	private ProvResourceMapBuilder() {
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
	public static ProvResourceMapBuilder getInstance() {
		if (instance == null) {
			instance = new ProvResourceMapBuilder();
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
	 * Adds a wasDerivedFrom triple to the specified Resource Map
	 * @param resourceMap
	 * @param primaryDataId
	 * @param derivedDataId
	 * @return
	 * @throws OREException
	 * @throws URISyntaxException
	 */
	public void addWasDerivedFrom(ResourceMap resourceMap, Identifier primaryDataId, Identifier derivedDataId)
	throws OREException, URISyntaxException{
		
		Triple triple = OREFactory.createTriple(
								new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(derivedDataId.getValue())), 
								PROV_WAS_DERIVED_FROM, 
								new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(primaryDataId.getValue())));
		resourceMap.addTriple(triple);				
	}
		
	/**
	 * Add multiple wasDerivedFrom triples to the specified Resource Map
	 * @param resourceMap
	 * @param idMap
	 * @return
	 * @throws OREException
	 * @throws URISyntaxException
	 */
	public void addWasDerivedFrom(ResourceMap resourceMap, Map<Identifier, List<Identifier>> idMap)
	throws OREException, URISyntaxException{
		
		//Iterate over each derived data ID
		for(Identifier derivedDataId: idMap.keySet()){
			//Get the list of primary data IDs
			List<Identifier> primaryDataIds = idMap.get(derivedDataId);
				for(Identifier primaryDataId: primaryDataIds){
				Triple triple = OREFactory.createTriple(
										new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(derivedDataId.getValue())), 
										PROV_WAS_DERIVED_FROM, 
										new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(primaryDataId.getValue())));
				resourceMap.addTriple(triple);
			}
		}		
	}
	
	
	/**
	 * Adds a wasGeneratedBy triple to the specified Resource Map
	 * @param resourceMap
	 * @param subjectId
	 * @param objectId
	 * @return
	 * @throws OREException
	 * @throws URISyntaxException
	 */
	public void addWasGeneratedBy(ResourceMap resourceMap, Identifier subjectId, Identifier objectId)
	throws OREException, URISyntaxException{
		
		Triple triple = OREFactory.createTriple(
								new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(subjectId.getValue())), 
								PROV_WAS_GENERATED_BY, 
								new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(objectId.getValue())));
		resourceMap.addTriple(triple);				
	}

	/**
	 * Add multiple addWasGeneratedBy triples to the specified Resource Map
	 * @param resourceMap
	 * @param idMap
	 * @return
	 * @throws OREException
	 * @throws URISyntaxException
	 */
	public void addWasGeneratedBy(ResourceMap resourceMap, Map<Identifier, List<Identifier>> idMap)
	throws OREException, URISyntaxException{
		
		//Iterate over each subject ID
		for(Identifier subjectId: idMap.keySet()){
			//Get the list of primary data IDs
			List<Identifier> objectIds = idMap.get(subjectId);
				for(Identifier objectId: objectIds){
				Triple triple = OREFactory.createTriple(
										new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(subjectId.getValue())), 
										PROV_WAS_GENERATED_BY, 
										new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(objectId.getValue())));
				resourceMap.addTriple(triple);
			}
		}		
	}
	
	
	/**
	 * Adds a addWasInformedBy triple to the specified Resource Map
	 * @param resourceMap
	 * @param subjectId
	 * @param objectId
	 * @return
	 * @throws OREException
	 * @throws URISyntaxException
	 */
	public void addWasInformedBy(ResourceMap resourceMap, Identifier subjectId, Identifier objectId)
	throws OREException, URISyntaxException{
		
		Triple triple = OREFactory.createTriple(
								new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(subjectId.getValue())), 
								PROV_WAS_INFORMED_BY, 
								new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(objectId.getValue())));
		resourceMap.addTriple(triple);				
	}

	/**
	 * Add multiple addWasInformedBy triples to the specified Resource Map
	 * @param resourceMap
	 * @param idMap
	 * @return
	 * @throws OREException
	 * @throws URISyntaxException
	 */
	public void addWasInformedBy(ResourceMap resourceMap, Map<Identifier, List<Identifier>> idMap)
	throws OREException, URISyntaxException{
		
		//Iterate over each subject ID
		for(Identifier subjectId: idMap.keySet()){
			//Get the list of primary data IDs
			List<Identifier> objectIds = idMap.get(subjectId);
				for(Identifier objectId: objectIds){
				Triple triple = OREFactory.createTriple(
										new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(subjectId.getValue())), 
										PROV_WAS_INFORMED_BY, 
										new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(objectId.getValue())));
				resourceMap.addTriple(triple);
			}
		}		
	}
	
	/**
	 * Adds a addUsed triple to the specified Resource Map
	 * @param resourceMap
	 * @param subjectId
	 * @param objectId
	 * @return
	 * @throws OREException
	 * @throws URISyntaxException
	 */
	public void addUsed(ResourceMap resourceMap, Identifier subjectId, Identifier objectId)
	throws OREException, URISyntaxException{
		
		Triple triple = OREFactory.createTriple(
								new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(subjectId.getValue())), 
								PROV_USED, 
								new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(objectId.getValue())));
		resourceMap.addTriple(triple);				
	}

	/**
	 * Add multiple addUsed triples to the specified Resource Map
	 * @param resourceMap
	 * @param idMap
	 * @return
	 * @throws OREException
	 * @throws URISyntaxException
	 */
	public void addUsed(ResourceMap resourceMap, Map<Identifier, List<Identifier>> idMap)
	throws OREException, URISyntaxException{
		
		//Iterate over each subject ID
		for(Identifier subjectId: idMap.keySet()){
			//Get the list of primary data IDs
			List<Identifier> objectIds = idMap.get(subjectId);
				for(Identifier objectId: objectIds){
				Triple triple = OREFactory.createTriple(
										new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(subjectId.getValue())), 
										PROV_USED, 
										new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(objectId.getValue())));
				resourceMap.addTriple(triple);
			}
		}		
	}

}
