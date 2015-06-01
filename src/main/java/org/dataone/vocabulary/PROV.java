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

/**
 * Provides select static terms for the PROV ontology used in ProvONE constructs
 * 
 */
public class PROV {

    protected static final String namespace = "http://www.w3.org/ns/prov#";
    
    public static final String prefix = "prov";

    protected static final List<String> properties = Arrays.asList(
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
    
    // Classes defined in PROV (ProvONE-relevant subset)
    public static final String Usage                = namespace + "Usage";
    public static final String Generation           = namespace + "Generation";
    public static final String Association          = namespace + "Association";
    public static final String Collection           = namespace + "Collection";

    // Object properties defined in PROV (ProvONE-relevant subset)
    public static final String wasDerivedFrom       = namespace + "wasDerivedFrom";
    public static final String used                 = namespace + "used";
    public static final String wasGeneratedBy       = namespace + "wasGeneratedBy";
    public static final String wasAssociatedWith    = namespace + "wasAssociatedWith";
    public static final String wasInformedBy        = namespace + "wasInformedBy";
    public static final String qualifiedGeneration  = namespace + "qualifiedGeneration";
    public static final String qualifiedAssociation = namespace + "qualifiedAssociation";
    public static final String agent                = namespace + "agent";
    public static final String hadPlan              = namespace + "hadPlan";
    public static final String qualifiedUsage       = namespace + "qualifiedUsage";
    public static final String hadMember            = namespace + "hadMember";

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
        predicate.setNamespace(namespace);
        predicate.setURI(new URI(namespace + property));
        
        return predicate;
        
    }

}

