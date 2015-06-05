/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright 2015
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
 * $Id: $
 */

package org.dataone.vocabulary;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import org.dspace.foresite.Predicate;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

/**
 * Provides select static terms for the PROV ontology used in ProvONE constructs
 * 
 */
public class PROV {

    public static final String namespace = "http://www.w3.org/ns/prov#";
    
    public static final String prefix = "prov";

    /** Classes defined in PROV (ProvONE-relevant subset) */
    public static final List<String> classes = Arrays.asList(
            "Usage",      
            "Generation",                
            "Association",      
            "Collection");
    
    /* Object properties defined in PROV (ProvONE-relevant subset) */
    public static final List<String> properties = Arrays.asList(
            "wasDerivedFrom",      
            "used",                
            "wasGeneratedBy",      
            "wasAssociatedWith",   
            "wasInformedBy",       
            "qualifiedGeneration", 
            "qualifiedAssociation",
            "agent",               
            "hadPlan",             
            "qualifiedUsage",      
            "hadMember");
    
    public static final Resource Usage                = resource("Usage");
    public static final Resource Generation           = resource("Generation");
    public static final Resource Association          = resource("Association");
    public static final Resource Collection           = resource("Collection");

    public static final Property wasDerivedFrom       = property("wasDerivedFrom");
    public static final Property used                 = property("used");
    public static final Property wasGeneratedBy       = property("wasGeneratedBy");
    public static final Property wasAssociatedWith    = property("wasAssociatedWith");
    public static final Property wasInformedBy        = property("wasInformedBy");
    public static final Property qualifiedGeneration  = property("qualifiedGeneration");
    public static final Property qualifiedAssociation = property("qualifiedAssociation");
    public static final Property agent                = property("agent");
    public static final Property hadPlan              = property("hadPlan");
    public static final Property qualifiedUsage       = property("qualifiedUsage");
    public static final Property hadMember            = property("hadMember");

    /**
     * For a given PROV property string, return a Predicate object with the URI, namespace,
     * and prefix fields set to the default values.
     * 
     * @param property  The name of the PROV object property to use as a Predicate
     * @return  The Predicate instance using the given property
     * 
     * @throws IllegalArgumentException
     * @throws URISyntaxException
     */
    public static Predicate predicate(String property) 
            throws IllegalArgumentException, URISyntaxException {
        
        if ( ! properties.contains(property) ) {
           throw new IllegalArgumentException("The given argument: " + property +
                   " is not a PROV property. Please use one of the follwing to " +
                   "create a Predicate: " + Arrays.toString(properties.toArray())); 
        }
        
        Predicate predicate = new Predicate();
        predicate.setPrefix(prefix);
        predicate.setName(property);
        predicate.setNamespace(namespace);
        predicate.setURI(new URI(namespace + property));
        
        return predicate;
        
    }
    
    /**
     * Return a Jena Resource instance for the given localName term
     * 
     * @param localName
     * @return  resource  The Resource for the term
     */
    protected static Resource resource(String localName) {
        return ResourceFactory.createResource(namespace + localName);
        
    }

    /**
     * Return a Jena Property instance for the given localName term
     * 
     * @param localName
     * @return  property  The Property for the term
     */
    protected static Property property(String localName) {
        return ResourceFactory.createProperty(namespace, localName);
        
    }


}

