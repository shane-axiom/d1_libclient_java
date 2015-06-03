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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.util.EncodingUtilities;
import org.dataone.vocabulary.PROV;
import org.dataone.vocabulary.ProvONE_V1;
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
import org.dspace.foresite.Vocab;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
/**
 *  A Resource Map builder with methods for adding provenance or other statements about
 *  resource in an ORE aggregation.  
 *  
 */
public class ProvResourceMapBuilder {
	
	// TODO: will this always resolve?
	private static final String D1_URI_PREFIX = Settings.getConfiguration()
			.getString("D1Client.CN_URL", "https://cn-dev.test.dataone.org/cn") + "/v1/resolve/";

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
	
	private static Model rdfModel = null;
	
	private static Log log = LogFactory.getLog(ProvResourceMapBuilder.class);
	
	private static final String CITO_NAMESPACE_URI = "http://purl.org/spar/cito/";

    private static final String DEFAULT_RDF_FORMAT = "RDF/XML";

	/*
	 * Initialize the ProvResourceMapBuilder, populating the available predicates list, etc.
	 */
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
		PROV_WAS_DERIVED_FROM = PROV.predicate("wasDerivedFrom");
		
		// create the PROV:wasGeneratedBy predicate
		PROV_WAS_GENERATED_BY = PROV.predicate("wasGeneratedBy");
		
		// create the PROV:wasInformedBy predicate
		PROV_WAS_INFORMED_BY = PROV.predicate("wasInformedBy");
		
		// create the PROV:used predicate
		PROV_USED = PROV.predicate("used");
		
		// create the PROV:wasAssociatedWith predicate
		PROV_WAS_ASSOCIATED_WITH = PROV.predicate("wasAssociatedWith");
		
		// create the PROV:qualifiedAssociation predicate
		PROV_QUALIFIED_ASSOCIATION = PROV.predicate("qualifiedAssociation");
		
		// create the PROV:agent predicate
		PROV_P_AGENT = PROV.predicate("agent");
		
		// create the PROV:hadPlan predicate
		PROV_HAD_PLAN = PROV.predicate("hadPlan");
		
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
		
		// Create and configure an RDF model to manipulate the resource map
        rdfModel = ModelFactory.createDefaultModel();
        rdfModel.setNsPrefix(PROV.prefix, PROV.namespace);
        rdfModel.setNsPrefix(ProvONE_V1.prefix, ProvONE_V1.namespace);
        rdfModel.setNsPrefix(CITO_DOCUMENTS.getPrefix(), CITO_DOCUMENTS.getNamespace());

	}
	
	public ProvResourceMapBuilder() {
		try {
			init();
			
		} catch (URISyntaxException e) {
			log.error("there was a problem during initialzation: " + e.getMessage());
			if (log.isDebugEnabled()) {
                e.printStackTrace();
                
            }
		}
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
	public ResourceMap addWasDerivedFrom(ResourceMap resourceMap, Identifier primaryDataId, Identifier derivedDataId)
	throws OREException, URISyntaxException{
		
		Triple triple = OREFactory.createTriple(
								new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(derivedDataId.getValue())), 
								PROV_WAS_DERIVED_FROM, 
								new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(primaryDataId.getValue())));
		resourceMap.addTriple(triple);
        
		return resourceMap;				
	}
		
	/**
	 * Add multiple wasDerivedFrom triples to the specified Resource Map
	 * @param resourceMap
	 * @param idMap
	 * @return
	 * @throws OREException
	 * @throws URISyntaxException
	 */
	public ResourceMap addWasDerivedFrom(ResourceMap resourceMap, Map<Identifier, List<Identifier>> idMap)
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
        
		return resourceMap;		
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
	public ResourceMap addWasGeneratedBy(ResourceMap resourceMap, Identifier subjectId, Identifier objectId)
	throws OREException, URISyntaxException{
		
		Triple triple = OREFactory.createTriple(
								new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(subjectId.getValue())), 
								PROV_WAS_GENERATED_BY, 
								new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(objectId.getValue())));
		resourceMap.addTriple(triple);
        
		return resourceMap;				
	}

	/**
	 * Add multiple addWasGeneratedBy triples to the specified Resource Map
	 * @param resourceMap
	 * @param idMap
	 * @return
	 * @throws OREException
	 * @throws URISyntaxException
	 */
	public ResourceMap addWasGeneratedBy(ResourceMap resourceMap, Map<Identifier, List<Identifier>> idMap)
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
        
		return resourceMap;		
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
	public ResourceMap addWasInformedBy(ResourceMap resourceMap, Identifier subjectId, Identifier objectId)
	throws OREException, URISyntaxException{
		
		Triple triple = OREFactory.createTriple(
								new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(subjectId.getValue())), 
								PROV_WAS_INFORMED_BY, 
								new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(objectId.getValue())));
		resourceMap.addTriple(triple);
        
		return resourceMap;				
	}

	/**
	 * Add multiple addWasInformedBy triples to the specified Resource Map
	 * @param resourceMap
	 * @param idMap
	 * @return
	 * @throws OREException
	 * @throws URISyntaxException
	 */
	public ResourceMap addWasInformedBy(ResourceMap resourceMap, Map<Identifier, List<Identifier>> idMap)
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
        
		return resourceMap;		
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
	public ResourceMap addUsed(ResourceMap resourceMap, Identifier subjectId, Identifier objectId)
	throws OREException, URISyntaxException{
		
		Triple triple = OREFactory.createTriple(
								new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(subjectId.getValue())), 
								PROV_USED, 
								new URI(D1_URI_PREFIX + EncodingUtilities.encodeUrlPathSegment(objectId.getValue())));
		resourceMap.addTriple(triple);
        
		return resourceMap;				
	}

	/**
	 * Add multiple addUsed triples to the specified Resource Map
	 * @param resourceMap
	 * @param idMap
	 * @return
	 * @throws OREException
	 * @throws URISyntaxException
	 */
	public ResourceMap addUsed(ResourceMap resourceMap, Map<Identifier, List<Identifier>> idMap)
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
        
		return resourceMap;		
	}

	/**
	 * Insert statements into the resource map graph using the given subject, predicate, and list
	 * of object URIs.  When inserting a relationship, ensure that either the subject or object URIs
	 * are connected in the graph (that one is present) to avoid an OREException.
	 * 
	 * @param resourceMap  The resource map to be augmented
	 * @param subject  The URI identifying the subject of each statement
	 * @param predicate  The Predicate identifying the relation of each statement
	 * @param objects  A list of URIs identifying the objects of each statement
	 * @return
	 * @throws OREException
	 */
	public ResourceMap insertRelationship(ResourceMap resourceMap, URI subject, Predicate predicate, 
	        List<URI> objects ) throws OREException {
	    
	    if ( subject == null ) {
	        throw new OREException("Subject cannot be null. Please set the subject URI.");
	        
	    }
	    
    	for ( URI object : objects ) {
    	    // Build the triple with the given predicate
            if ( object == null ) {
                throw new OREException("Object cannot be null. Please set the object URI.");
                
            }
            
    	    Triple triple = OREFactory.createTriple(subject, predicate, object);
    	    resourceMap.addTriple(triple);
    	}
    	
    	setModel(resourceMap);
    	
    	return getModel();
	}

    /*
     * For the given resource map, add it to the oreModel so it can be manipulated
     * outside of the ORE API (with the Jena API)
     * 
     * @param resourceMap
     */
    private void setModel(ResourceMap resourceMap) throws OREException {
        ORESerialiser serialiser = ORESerialiserFactory.getInstance(DEFAULT_RDF_FORMAT);
    	try {
    	    ResourceMapDocument oreDocument = serialiser.serialise(resourceMap);
    	    InputStream inputStream = IOUtils.toInputStream(oreDocument.toString());
            rdfModel.read(inputStream, null); // TODO: Do we need to handle relative URIs?
            
        } catch (ORESerialiserException e) {
            
            if ( log.isDebugEnabled() ) {
                e.printStackTrace();
                
            }
            throw new OREException(e.getCause());
        }
    }

    /**
     * Return the RDF model as a ResourceMap.  This converts the more general RDF model into an
     * ORE specific resource map object.
     * 
     * @return resourceMap  The RDF model as a resource map
     * @throws OREException
     */
    public ResourceMap getModel() throws OREException {
        ResourceMap resourceMap = null;
        
        OREParser oreParser = OREParserFactory.getInstance(DEFAULT_RDF_FORMAT);
        // Do some odd gymnastics to go from an output stream to an input stream
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        rdfModel.write(outputStream);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        try {
            resourceMap = oreParser.parse(inputStream);
            
        } catch (OREParserException e) {
            if ( log.isDebugEnabled() ) {
                e.printStackTrace();
                
            }
            throw new OREException(e.getCause());
        }
        
        if ( log.isDebugEnabled() ) {
            log.debug(outputStream);
        }
        return resourceMap;
        
    }
}
