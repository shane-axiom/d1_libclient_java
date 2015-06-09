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
 * Provides select static terms for the 2008/02/11 Dublin Core Metadata Terms 
 * used in DataONE constructs
 * 
 */
public class DC_TERMS_080211 {

    public static final String namespace = "http://purl.org/dc/terms/";
    
    public static final String prefix = "dcterms";

    /** Classes defined in Dublin Core Metadata Terms */
    public static final List<String> classes = Arrays.asList(
            "Agent",
            "AgentClass",
            "BibliographicResource",
            "FileFormat",
            "Frequency",
            "Jurisdiction",
            "LicenseDocument",
            "LinguisticSystem",
            "Location",
            "LocationPeriodOrJurisdiction",
            "MediaType",
            "MediaTypeOrExtent",
            "MethodOfAccrual",
            "MethodOfInstruction",
            "PeriodOfTime",
            "PhysicalMedium",
            "PhysicalResource",
            "Policy",
            "ProvenanceStatement",
            "RightsStatement",
            "SizeOrDuration",
            "Standard");
    
    /* Object properties defined in Dublin Core Terms */
    public static final List<String> properties = Arrays.asList(
            "abstract",
            "accessRights",
            "accrualMethod",
            "accrualPeriodicity",
            "accrualPolicy",
            "alternative",
            "audience",
            "available",
            "bibliographicCitation",
            "conformsTo",
            "contributor",
            "coverage",
            "created",
            "creator",
            "date",
            "dateAccepted",
            "dateCopyrighted",
            "dateSubmitted",
            "description",
            "educationLevel",
            "extent",
            "format",
            "hasFormat",
            "hasPart",
            "hasVersion",
            "identifier",
            "instructionalMethod",
            "isFormatOf",
            "isPartOf",
            "isReferencedBy",
            "isReplacedBy",
            "isRequiredBy",
            "issued",
            "isVersionOf",
            "language",
            "license",
            "mediator",
            "medium",
            "modified",
            "provenance",
            "publisher",
            "references",
            "relation",
            "replaces",
            "requires",
            "rights",
            "rightsHolder",
            "source",
            "spatial",
            "subject",
            "tableOfContents",
            "temporal",
            "title",
            "type",
            "valid");
    
    public static final Resource Agent                        = resource("Agent");
    public static final Resource AgentClass                   = resource("AgentClass");
    public static final Resource BibliographicResource        = resource("BibliographicResource");
    public static final Resource FileFormat                   = resource("FileFormat");
    public static final Resource Frequency                    = resource("Frequency");
    public static final Resource Jurisdiction                 = resource("Jurisdiction");
    public static final Resource LicenseDocument              = resource("LicenseDocument");
    public static final Resource LinguisticSystem             = resource("LinguisticSystem");
    public static final Resource Location                     = resource("Location");
    public static final Resource LocationPeriodOrJurisdiction = resource("LocationPeriodOrJurisdiction");
    public static final Resource MediaType                    = resource("MediaType");
    public static final Resource MediaTypeOrExtent            = resource("MediaTypeOrExtent");
    public static final Resource MethodOfAccrual              = resource("MethodOfAccrual");
    public static final Resource MethodOfInstruction          = resource("MethodOfInstruction");
    public static final Resource PeriodOfTime                 = resource("PeriodOfTime");
    public static final Resource PhysicalMedium               = resource("PhysicalMedium");
    public static final Resource PhysicalResource             = resource("PhysicalResource");
    public static final Resource Policy                       = resource("Policy");
    public static final Resource ProvenanceStatement          = resource("ProvenanceStatement");
    public static final Resource RightsStatement              = resource("RightsStatement");
    public static final Resource SizeOrDuration               = resource("SizeOrDuration");
    public static final Resource Standard                     = resource("Standard");

    public static final Property abstracT              = property("abstract");
    public static final Property accessRights          = property("accessRights");
    public static final Property accrualMethod         = property("accrualMethod");
    public static final Property accrualPeriodicity    = property("accrualPeriodicity");
    public static final Property accrualPolicy         = property("accrualPolicy");
    public static final Property alternative           = property("alternative");
    public static final Property audience              = property("audience");
    public static final Property available             = property("available");
    public static final Property bibliographicCitation = property("bibliographicCitation");
    public static final Property conformsTo            = property("conformsTo");
    public static final Property contributor           = property("contributor");
    public static final Property coverage              = property("coverage");
    public static final Property created               = property("created");
    public static final Property creator               = property("creator");
    public static final Property date                  = property("date");
    public static final Property dateAccepted          = property("dateAccepted");
    public static final Property dateCopyrighted       = property("dateCopyrighted");
    public static final Property dateSubmitted         = property("dateSubmitted");
    public static final Property description           = property("description");
    public static final Property educationLevel        = property("educationLevel");
    public static final Property extent                = property("extent");
    public static final Property format                = property("format");
    public static final Property hasFormat             = property("hasFormat");
    public static final Property hasPart               = property("hasPart");
    public static final Property hasVersion            = property("hasVersion");
    public static final Property identifier            = property("identifier");
    public static final Property instructionalMethod   = property("instructionalMethod");
    public static final Property isFormatOf            = property("isFormatOf");
    public static final Property isPartOf              = property("isPartOf");
    public static final Property isReferencedBy        = property("isReferencedBy");
    public static final Property isReplacedBy          = property("isReplacedBy");
    public static final Property isRequiredBy          = property("isRequiredBy");
    public static final Property issued                = property("issued");
    public static final Property isVersionOf           = property("isVersionOf");
    public static final Property language              = property("language");
    public static final Property license               = property("license");
    public static final Property mediator              = property("mediator");
    public static final Property medium                = property("medium");
    public static final Property modified              = property("modified");
    public static final Property provenance            = property("provenance");
    public static final Property publisher             = property("publisher");
    public static final Property references            = property("references");
    public static final Property relation              = property("relation");
    public static final Property replaces              = property("replaces");
    public static final Property requires              = property("requires");
    public static final Property rights                = property("rights");
    public static final Property rightsHolder          = property("rightsHolder");
    public static final Property source                = property("source");
    public static final Property spatial               = property("spatial");
    public static final Property subject               = property("subject");
    public static final Property tableOfContents       = property("tableOfContents");
    public static final Property temporal              = property("temporal");
    public static final Property title                 = property("title");
    public static final Property type                  = property("type");
    public static final Property valid                 = property("valid");

    /**
     * For a given DC Terms property string, return a Predicate object with the URI, namespace,
     * and prefix fields set to the default values.
     * 
     * @param property  The name of the DC Terms object property to use as a Predicate
     * @return  The Predicate instance using the given property
     * 
     * @throws IllegalArgumentException
     * @throws URISyntaxException
     */
    public static Predicate predicate(String property) 
            throws IllegalArgumentException, URISyntaxException {
        
        if ( ! properties.contains(property) ) {
           throw new IllegalArgumentException("The given argument: " + property +
                   " is not a DC Terms property. Please use one of the follwing to " +
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

