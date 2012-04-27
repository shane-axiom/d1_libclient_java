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
package org.dataone.eml;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.ObjectFormatCache;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.ObjectFormat;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * @author berkley
 * This class parses EML documents to pull any relevant D1 information
 * for systemMetadata creation and other purposes.
 */
public class DataoneEMLParser
{
    private static DataoneEMLParser parser = null;
    
    protected static Log log = LogFactory.getLog(DataoneEMLParser.class);
    
    protected static TreeSet<String> supportedFormatIdentifiers;
    protected static String supportedNamespaceSummary;
    
    /**
     * private constructor.  To create a new parser, use getInstance().
     */
    private DataoneEMLParser()
    {
        supportedFormatIdentifiers = new TreeSet<String>();
        supportedFormatIdentifiers.add("eml://ecoinformatics.org/eml-2.0.0");
        supportedFormatIdentifiers.add("eml://ecoinformatics.org/eml-2.0.1");
        supportedFormatIdentifiers.add("eml://ecoinformatics.org/eml-2.1.0");
        supportedFormatIdentifiers.add("eml://ecoinformatics.org/eml-2.1.1");
        supportedNamespaceSummary = "EML 2.0.0, 2.0.1, 2.1.0, and 2.1.1";
    }

    /**
     * singleton accessor
     * @return a parser to parse EML documents
     */
    public static DataoneEMLParser getInstance()
    {
        if(parser == null)
        {
            parser = new DataoneEMLParser();
        }
        return parser;
    }
    
    /**
     * package level method to allow testing of embedded identifiers against
     * the deployed objectFormatFile (and in-context integration tests)
     * @return
     */
    Iterator<String> getSupportedFormatIdentifierIterator() {
    	return supportedFormatIdentifiers.iterator();
    }
    
    /**
     * parse an eml document and return any distribution urls
     * @param is
     * @throws XPathExpressionException 
     * @throws NotFound 
     * @throws ServiceFailure 
     * @throws NotImplemented 
     */
    public EMLDocument parseDocument(InputStream is)
        throws ParserConfigurationException, IOException, SAXException, 
        XPathExpressionException //, NotFound, ServiceFailure, NotImplemented
    {
        //info we need:
        //1) any distribution urls
        //2) doctype (public_id)
        //3) mime type for each 1
        
        log.debug("parsing EML document for any distribution urls");
        DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
        builderFactory.setNamespaceAware(true);
        
        Document d = builderFactory.newDocumentBuilder().parse(is);
        String namespace = d.getFirstChild().getNamespaceURI();
        log.debug("namespace: " + namespace);
        

        if(namespace == null)
        {
            log.error("WARNING: No namespace is declared.  Cannot parse this document.");
            return null;
        }

        if (!supportedFormatIdentifiers.contains(namespace)) {
        	throw new ParserConfigurationException(
                    "This parser only parses " + supportedNamespaceSummary + ".  Namespace " + 
                    namespace + " is not supported.");
        }
        	
        ObjectFormatIdentifier namespaceFormatId = new ObjectFormatIdentifier();
        namespaceFormatId.setValue(namespace);

        EMLDocument emlDoc = null;
        try {
        	ObjectFormat format = ObjectFormatCache.getInstance().getFormat(namespaceFormatId);
        	emlDoc = parseEMLDocument(d,format);
        } 
        catch (BaseException e) {
        	throw new ParserConfigurationException(
                    "Unexpected Error validating the format:: " + e.getClass().getSimpleName() 
                    + "( " + e.getDetail_code() + " ): " + e.getDescription());
        }
        return emlDoc;
    }
    
    
    private NodeList runXPath(String expression, Node n)
      throws XPathExpressionException
    {
        XPathFactory factory = XPathFactory.newInstance();
        XPath xpath = factory.newXPath();
        XPathExpression expr = xpath.compile(expression);
        NodeList result = (org.w3c.dom.NodeList)expr.evaluate(n, XPathConstants.NODESET);
        return result;
    }
    

    
    private EMLDocument parseEMLDocument(Document doc, ObjectFormat docType) 
      throws XPathExpressionException, ServiceFailure, NotImplemented
    {
     	
    	log.debug("DataoneEMLParser.parseEMLDocument() called.");
    	log.debug("Parsing a document of format: " + docType.getFormatName());
        
        EMLDocument emlDoc = new EMLDocument();
        NodeList result = runXPath("//distribution", doc);        
        
        log.debug("result: " + result);
        if(result != null)
        {
            for (int i = 0; i < result.getLength(); i++) 
            {
                // find the data URL
                Node n = result.item(i);
                NodeList nl = runXPath("online/url", n);
                if(nl.getLength() == 0)
                {
                    continue;
                }
                Node firstChild = nl.item(0).getFirstChild();
                if(firstChild == null)
                {
                    continue;
                }
                String url = firstChild.getNodeValue();
                
                // determine the data mime type
                String mimeType = "";
                Node physicalNode = result.item(i).getParentNode();
                NodeList nl1 = runXPath("dataFormat/textFormat", physicalNode);
                NodeList nl2 = runXPath("dataFormat/binaryRasterFormat", physicalNode);
                NodeList nl3 = runXPath("dataFormat/externallyDefinedFormat", physicalNode);
                //TODO: this isn't entirely right, but it's a good start.  Need to 
                //figure out how to property parse the EML to get a better idea
                //of what the mime type is
                
                ObjectFormatIdentifier formatId = new ObjectFormatIdentifier();
                
                if (nl1.getLength() > 0) { //found a text format
                    log.debug("Found a text format");
                    formatId.setValue("text/plain");
                    NodeList nl4 = runXPath("dataFormat/textFormat/simpleDelimited", physicalNode);
                    
                    // look for CSV files
                    if ( nl4.getLength() > 0 ) {
                      log.debug("Found a csv format");
                      formatId.setValue("text/csv");
                    }
                }
                else if (nl2.getLength() > 0) {
                    //TODO: could do a bit more parsing and refine this type more
                    log.debug("Found a binary raster format");
                    formatId.setValue("application/octet-stream");
                }
                else if (nl3.getLength() > 0) {
                    // it's possible that the mime type is in this field.  
                    // Check for it and set the object format
                    log.debug("Found an externally defined format");
                    formatId.setValue("application/octet-stream");
                    NodeList nl5 = runXPath("dataFormat/externallyDefinedFormat/formatName", physicalNode);
                    if ( nl5.getLength() > 0 ) {
                      Node childNode1 = nl5.item(0).getFirstChild();
                      String formatName = childNode1.getNodeValue();
                      
                      // set the object format if it exists
                      formatId.setValue(formatName);
                    }
                    
                }
                
                try {
                	mimeType = ObjectFormatCache.getInstance().getFormat(formatId).getFormatId().getValue();
                } catch (NotFound nfe) {
                	// we need to continue in this case, we just won't know what the "official" D1 objectformat is
					log.error("Could not find object format for: " + formatId);
				}

                log.debug("mime type: " + mimeType); 
                log.debug("url:       " + url);
                emlDoc.addDistributionMetadata(url, mimeType);
            }
        }
        
        emlDoc.setObjectFormat(docType);
        log.debug("EML document type: " + emlDoc.format.toString());
        
        return emlDoc;
    }
}
