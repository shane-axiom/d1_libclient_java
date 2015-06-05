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
 * Provides select static terms for the CITO 2.6.3 ontology used in DataONE constructs
 * 
 */
public class CITO_263 {

    public static final String namespace = "http://purl.org/spar/cito/";
    
    public static final String prefix = "cito";

    /** Classes defined in PROV (ProvONE-relevant subset) */
    public static final List<String> classes = Arrays.asList(
            "CitationAct");
    
    /* Object properties defined in PROV (ProvONE-relevant subset) */
    public static final List<String> properties = Arrays.asList(
            "agreesWith",
            "cites",
            "citesAsAuthority",
            "citesAsDataSource",
            "citesAsEvidence",
            "citesAsMetadataDocument",
            "citesAsPotentialSolution",
            "citesAsRecommendedReading",
            "citesAsRelated",
            "citesAsSourceDocument",
            "citesForInformation",
            "compiles",
            "confirms",
            "containsAssertionFrom",
            "corrects",
            "credits",
            "critiques",
            "derides",
            "describes",
            "disagreesWith",
            "discusses",
            "disputes",
            "documents",
            "extends",
            "givesBackgroundTo",
            "givesSupportTo",
            "hasCitationCharacterization",
            "hasCitedEntity",
            "hasCitingEntity",
            "hasReplyFrom",
            "includesExcerptFrom",
            "includesQuotationFrom",
            "isAgreedWithBy",
            "isCitedAsAuthorityBy",
            "isCitedAsDataSourceBy",
            "isCitedAsEvidenceBy",
            "isCitedAsMetadataDocumentBy",
            "isCitedAsPontentialSolutionBy",
            "isCitedAsRecommendedReading",
            "isCitedAsRelatedBy",
            "isCitedAsSourceDocumentBy",
            "isCitedBy",
            "isCitedForInformationBy",
            "isCompiledBy",
            "isConfirmedBy",
            "isCorrectedBy",
            "isCreditedBy",
            "isCritiquedBy",
            "isDeridedBy",
            "isDescribedBy",
            "isDisagreedWithBy",
            "isDiscussedBy",
            "isDisputedBy",
            "isDocumentedBy",
            "isExtendedBy",
            "isParodiedBy",
            "isPlagiarizedBy",
            "isQualifiedBy",
            "isRefutedBy",
            "isRetractedBy",
            "isReviewedBy",
            "isRidiculedBy",
            "isSpeculatedOnBy",
            "isSupportedBy",
            "isUpdatedBy",
            "likes",
            "obtainsBackgroundFrom",
            "obtainsSupportFrom",
            "parodies",
            "plagiarizes",
            "providesAssertionFor",
            "providesConclusionsFor",
            "providesDataFor",
            "providesExcerptFor",
            "providesMethodFor",
            "providesQuotationFor",
            "qualifies",
            "refutes",
            "repliesTo",
            "retracts",
            "reviews",
            "ridicules",
            "sharesAuthorInstitutionWith",
            "sharesAuthorsWith",
            "sharesFundingAgencyWith",
            "speculatesOn",
            "supports",
            "updates",
            "usesConclusionsFrom",
            "usesDataFrom",
            "usesMethodIn");
    
    public static final Resource Usage = resource("CitationAct");

    public static final Property agreesWith                      = property("agreesWith");
    public static final Property cites                           = property("cites");
    public static final Property citesAsAuthority                = property("citesAsAuthority");
    public static final Property citesAsDataSource               = property("citesAsDataSource");
    public static final Property citesAsEvidence                 = property("citesAsEvidence");
    public static final Property citesAsMetadataDocument         = property("citesAsMetadataDocument");
    public static final Property citesAsPotentialSolution        = property("citesAsPotentialSolution");
    public static final Property citesAsRecommendedReading       = property("citesAsRecommendedReading");
    public static final Property citesAsRelated                  = property("citesAsRelated");
    public static final Property citesAsSourceDocument           = property("citesAsSourceDocument");
    public static final Property citesForInformation             = property("citesForInformation");
    public static final Property compiles                        = property("compiles");
    public static final Property confirms                        = property("confirms");
    public static final Property containsAssertionFrom           = property("containsAssertionFrom");
    public static final Property corrects                        = property("corrects");
    public static final Property credits                         = property("credits");
    public static final Property critiques                       = property("critiques");
    public static final Property derides                         = property("derides");
    public static final Property describes                       = property("describes");
    public static final Property disagreesWith                   = property("disagreesWith");
    public static final Property discusses                       = property("discusses");
    public static final Property disputes                        = property("disputes");
    public static final Property documents                       = property("documents");
    public static final Property extendS                         = property("extends");
    public static final Property givesBackgroundTo               = property("givesBackgroundTo");
    public static final Property givesSupportTo                  = property("givesSupportTo");
    public static final Property hasCitationCharacterization     = property("hasCitationCharacterization");
    public static final Property hasCitedEntity                  = property("hasCitedEntity");
    public static final Property hasCitingEntity                 = property("hasCitingEntity");
    public static final Property hasReplyFrom                    = property("hasReplyFrom");
    public static final Property includesExcerptFrom             = property("includesExcerptFrom");
    public static final Property includesQuotationFrom           = property("includesQuotationFrom");
    public static final Property isAgreedWithBy                  = property("isAgreedWithBy");
    public static final Property isCitedAsAuthorityBy            = property("isCitedAsAuthorityBy");
    public static final Property isCitedAsDataSourceBy           = property("isCitedAsDataSourceBy");
    public static final Property isCitedAsEvidenceBy             = property("isCitedAsEvidenceBy");
    public static final Property isCitedAsMetadataDocumentBy     = property("isCitedAsMetadataDocumentBy");
    public static final Property isCitedAsPontentialSolutionBy   = property("isCitedAsPontentialSolutionBy");
    public static final Property isCitedAsRecommendedReading     = property("isCitedAsRecommendedReading");
    public static final Property isCitedAsRelatedBy              = property("isCitedAsRelatedBy");
    public static final Property isCitedAsSourceDocumentBy       = property("isCitedAsSourceDocumentBy");
    public static final Property isCitedBy                       = property("isCitedBy");
    public static final Property isCitedForInformationBy         = property("isCitedForInformationBy");
    public static final Property isCompiledBy                    = property("isCompiledBy");
    public static final Property isConfirmedBy                   = property("isConfirmedBy");
    public static final Property isCorrectedBy                   = property("isCorrectedBy");
    public static final Property isCreditedBy                    = property("isCreditedBy");
    public static final Property isCritiquedBy                   = property("isCritiquedBy");
    public static final Property isDeridedBy                     = property("isDeridedBy");
    public static final Property isDescribedBy                   = property("isDescribedBy");
    public static final Property isDisagreedWithBy               = property("isDisagreedWithBy");
    public static final Property isDiscussedBy                   = property("isDiscussedBy");
    public static final Property isDisputedBy                    = property("isDisputedBy");
    public static final Property isDocumentedBy                  = property("isDocumentedBy");
    public static final Property isExtendedBy                    = property("isExtendedBy");
    public static final Property isParodiedBy                    = property("isParodiedBy");
    public static final Property isPlagiarizedBy                 = property("isPlagiarizedBy");
    public static final Property isQualifiedBy                   = property("isQualifiedBy");
    public static final Property isRefutedBy                     = property("isRefutedBy");
    public static final Property isRetractedBy                   = property("isRetractedBy");
    public static final Property isReviewedBy                    = property("isReviewedBy");
    public static final Property isRidiculedBy                   = property("isRidiculedBy");
    public static final Property isSpeculatedOnBy                = property("isSpeculatedOnBy");
    public static final Property isSupportedBy                   = property("isSupportedBy");
    public static final Property isUpdatedBy                     = property("isUpdatedBy");
    public static final Property likes                           = property("likes");
    public static final Property obtainsBackgroundFrom           = property("obtainsBackgroundFrom");
    public static final Property obtainsSupportFrom              = property("obtainsSupportFrom");
    public static final Property parodies                        = property("parodies");
    public static final Property plagiarizes                     = property("plagiarizes");
    public static final Property providesAssertionFor            = property("providesAssertionFor");
    public static final Property providesConclusionsFor          = property("providesConclusionsFor");
    public static final Property providesDataFor                 = property("providesDataFor");
    public static final Property providesExcerptFor              = property("providesExcerptFor");
    public static final Property providesMethodFor               = property("providesMethodFor");
    public static final Property providesQuotationFor            = property("providesQuotationFor");
    public static final Property qualifies                       = property("qualifies");
    public static final Property refutes                         = property("refutes");
    public static final Property repliesTo                       = property("repliesTo");
    public static final Property retracts                        = property("retracts");
    public static final Property reviews                         = property("reviews");
    public static final Property ridicules                       = property("ridicules");
    public static final Property sharesAuthorInstitutionWith     = property("sharesAuthorInstitutionWith");
    public static final Property sharesAuthorsWith               = property("sharesAuthorsWith");
    public static final Property sharesFundingAgencyWith         = property("sharesFundingAgencyWith");
    public static final Property speculatesOn                    = property("speculatesOn");
    public static final Property supports                        = property("supports");
    public static final Property updates                         = property("updates");
    public static final Property usesConclusionsFrom             = property("usesConclusionsFrom");
    public static final Property usesDataFrom                    = property("usesDataFrom");
    public static final Property usesMethodIn                    = property("usesMethodIn");
    /**
     * For a given CITO property string, return a Predicate object with the URI, namespace,
     * and prefix fields set to the default values.
     * 
     * @param property  The name of the CITO object property to use as a Predicate
     * @return  The Predicate instance using the given property
     * 
     * @throws IllegalArgumentException
     * @throws URISyntaxException
     */
    public static Predicate predicate(String property) 
            throws IllegalArgumentException, URISyntaxException {
        
        if ( ! properties.contains(property) ) {
           throw new IllegalArgumentException("The given argument: " + property +
                   " is not a CITO property. Please use one of the follwing to " +
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

