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
 * Provides static terms for the ProvONE extensions to the PROV ontology
 * @author cjones
 *
 */
public class ProvONE_V1 {

    /** the ProvONE namespace URI */
    protected static final String namespace = 
        "https://purl.org/dataone/ontologies/provenance/ProvONE/v1/owl/provone.owl#";
    
    /** A resonable ProvONE namespace prefix */
    public static final String prefix = "provone";

    /** The known object properties in the ProvONE model */
    protected static final List<String> properties = Arrays.asList(
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
    public static final String Program         = namespace + "Program";
    public static final String Port            = namespace + "Port";
    public static final String Channel         = namespace + "Channel";
    public static final String Controller      = namespace + "Controller";
    public static final String Workflow        = namespace + "Workflow";
    public static final String Execution       = namespace + "Execution";
    public static final String User            = namespace + "User";
    public static final String Data            = namespace + "Data";
    public static final String Visualization   = namespace + "Visualization";
    public static final String Document        = namespace + "Document";

    /** Object properties defined in the ProvONE model */
    public static final String hasSubProgram   = namespace + "hasSubProgram";
    public static final String controlledBy    = namespace + "controlledBy";
    public static final String controls        = namespace + "controls";
    public static final String hasInPort       = namespace + "hasInPort";
    public static final String hasOutPort      = namespace + "hasOutPort";
    public static final String hasDefaultParam = namespace + "hasDefaultParam";
    public static final String connectsTo      = namespace + "connectsTo";
    public static final String wasPartOf       = namespace + "wasPartOf";
    public static final String hadInPort       = namespace + "hadInPort";
    public static final String hadEntity       = namespace + "hadEntity";
    public static final String hadOutPort      = namespace + "hadOutPort";

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
}

