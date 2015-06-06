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
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.util.EncodingUtilities;
import org.dataone.vocabulary.CITO;
import org.dataone.vocabulary.PROV;
import org.dataone.vocabulary.ProvONE;
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

import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
/**
 *  A Resource Map builder with methods for adding provenance or other statements about
 *  resource in an ORE aggregation.  
 *  
 */
public class ProvResourceMapBuilder {
	
	// TODO: will this always resolve?
	private static final String D1_URI_PREFIX = Settings.getConfiguration()
			.getString("D1Client.CN_URL", "https://cn-dev.test.dataone.org/cn") + "/v1/resolve/";
	
	private Model rdfModel = null;
	
	private static Log log = LogFactory.getLog(ProvResourceMapBuilder.class);
	
    private static final String DEFAULT_RDF_FORMAT = "RDF/XML";

	public ProvResourceMapBuilder() {
	    // Create and configure an RDF model to manipulate the resource map
        rdfModel = ModelFactory.createDefaultModel();
        setNamespacePrefixes();

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
								PROV.predicate("wasDerivedFrom"), 
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
		                                PROV.predicate("wasDerivedFrom"), 
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
                                PROV.predicate("wasGeneratedBy"), 
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
		                                PROV.predicate("wasGeneratedBy"), 
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
                                PROV.predicate("wasInformedBy"), 
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
		                                PROV.predicate("wasInformedBy"), 
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
                                PROV.predicate("used"), 
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
		                                PROV.predicate("used"), 
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
	public ResourceMap insertRelationship(ResourceMap resourceMap, Resource subject, Property predicate, 
	        List<RDFNode> objects ) throws OREException {

	    setModel(resourceMap);

	    if ( subject == null ) {
	        throw new OREException("Subject cannot be null. Please set the subject Resource.");
	        
	    }
	    
    	for ( RDFNode object : objects ) {
    	    // Build the triple with the given predicate
            if ( object == null ) {
                throw new OREException("Object cannot be null. Please set the object Resource.");
                
            }
            Statement statement = rdfModel.createStatement(subject, predicate, object);
            rdfModel.add(statement);
    	}
    	    	
    	return getModel();
	}

	/**
	 * Insert statements into the resource map graph using a blank (anonymous) node ID 
	 * as the subject, a predicate, and a list of object URIs. The blankSubjectID should
	 * be unique to other blank nodes in the resource map. When inserting a relationship, 
	 * ensure that either the subject or object URIs are connected in the graph 
	 * (that one is present) to avoid an OREException.
	 * 
	 * @param resourceMap
	 * @param blankSubjectID
	 * @param predicate
	 * @param objects
	 * @return
	 * @throws OREException
	 */
	public ResourceMap insertRelationship(ResourceMap resourceMap, String blankSubjectID, 
	        Predicate predicate, List<URI> objects) throws OREException {

	    setModel(resourceMap);
        
	    if ( blankSubjectID == null || blankSubjectID.isEmpty() ) {
	         throw new OREException("blankSubjectID cannot be null or empty. Please set the blankSubjectID.");
	         
	    }
	     
	    for ( URI object : objects ) {
	        // Build the triple with the given blank node id, predicate, and object
	        if ( object == null ) {
	            throw new OREException("Object cannot be null. Please set the object URI.");
	            
	        }
	        
	        
	        AnonId anonId = new AnonId(blankSubjectID);
	        Resource blankSubject = rdfModel.createResource(anonId);
	        Property property = rdfModel.createProperty(predicate.getNamespace(), predicate.getName());
	        Resource objectNode = rdfModel.createResource(object.toString());
	        Statement statement = rdfModel.createStatement(blankSubject, property, objectNode);
	        rdfModel.add(statement);
	    }
	    
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
        setNamespacePrefixes();
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
        
        return resourceMap;
        
    }
    
    /*
     * For readability, reset the namespace prefixes in the model for ones 
     * we care about (PROV, ProvONE, CITO)
     */
    private void setNamespacePrefixes() {
        
        // Set the PROV prefix
        String provPrefix = rdfModel.getNsURIPrefix(PROV.namespace);
        rdfModel.removeNsPrefix(provPrefix);
        rdfModel.setNsPrefix(PROV.prefix, PROV.namespace);
        
        // Set the ProvONE prefix
        String provonePrefix = rdfModel.getNsURIPrefix(ProvONE.namespace);
        rdfModel.removeNsPrefix(provonePrefix);
        rdfModel.setNsPrefix(ProvONE.prefix, ProvONE.namespace);
        
        String citoPrefix = rdfModel.getNsURIPrefix(CITO.prefix);
        rdfModel.removeNsPrefix(citoPrefix);
        rdfModel.setNsPrefix(CITO.prefix, CITO.namespace);
    }
}
