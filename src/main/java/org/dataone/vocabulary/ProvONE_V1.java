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
 * Provides static terms for the ProvONE extensions to the PROV ontology
 * @author cjones
 *
 */
public class ProvONE_V1 {

    /** the ProvONE namespace URI */
    public static final String namespace = 
        "https://purl.org/dataone/ontologies/provenance/ProvONE/v1/owl/provone.owl#";
    
    /** A resonable ProvONE namespace prefix */
    public static final String prefix = "provone";

    /** The known object properties in the ProvONE model */
    public static final List<String> properties = Arrays.asList(
        "hasSubProgram",   
        "controlledBy",     
        "controls",    
        "hasInPort",        
        "hasOutPort",       
        "hasDefaultParam",  
        "connectsTo",       
        "wasPartOf",        
        "hadInPort",        
        "hadEntity",        
        "hadOutPort");
    
    /** Classes defined in the ProvONE model */
    public static final Resource Program         = resource("Program");
    public static final Resource Port            = resource("Port");
    public static final Resource Channel         = resource("Channel");
    public static final Resource Controller      = resource("Controller");
    public static final Resource Workflow        = resource("Workflow");
    public static final Resource Execution       = resource("Execution");
    public static final Resource User            = resource("User");
    public static final Resource Data            = resource("Data");
    public static final Resource Visualization   = resource("Visualization");
    public static final Resource Document        = resource("Document");

    /** Object properties defined in the ProvONE model */
    public static final Property hasSubProgram   = property("hasSubProgram");
    public static final Property controlledBy    = property("controlledBy");
    public static final Property controls        = property("controls");
    public static final Property hasInPort       = property("hasInPort");
    public static final Property hasOutPort      = property("hasOutPort");
    public static final Property hasDefaultParam = property("hasDefaultParam");
    public static final Property connectsTo      = property("connectsTo");
    public static final Property wasPartOf       = property("wasPartOf");
    public static final Property hadInPort       = property("hadInPort");
    public static final Property hadEntity       = property("hadEntity");
    public static final Property hadOutPort      = property("hadOutPort");

    /**
     * For a given ProvONE property string, return a Predicate object with the URI, namespace,
     * and prefix fields set to the default values.
     * 
     * @param property  The name of the ProvONE object property to use as a Predicate
     * @return  The Predicate instance using the given property
     * 
     * @throws IllegalArgumentException
     * @throws URISyntaxException
     */
    public static Predicate predicate(String property) 
            throws IllegalArgumentException, URISyntaxException {
        
        if ( ! properties.contains(property) ) {
           throw new IllegalArgumentException("The given argument: " + property +
                   " is not a ProvONE property. Please use one of the follwing to " +
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

