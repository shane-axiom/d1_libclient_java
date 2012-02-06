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
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.types.v1.ObjectFormat;
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
    
    /**
     * private constructor.  To create a new parser, use getInstance().
     */
    private DataoneEMLParser()
    {
        
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
     * parse an eml document and return any distribution urls
     * @param is
     * @throws XPathExpressionException 
     * @throws NotFound 
     */
    public EMLDocument parseDocument(InputStream is)
        throws ParserConfigurationException, IOException, SAXException, 
        XPathExpressionException, NotFound
    {
        //info we need:
        //1) any distribution urls
        //2) doctype (public_id)
        //3) mime type for each 1
        EMLDocument doc = new EMLDocument();
        
        log.debug("parsing EML document for any distribution urls");
        DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
        builderFactory.setNamespaceAware(true);
        
        Document d = builderFactory.newDocumentBuilder().parse(is);
        String namespace = d.getFirstChild().getNamespaceURI();
        log.debug("namespace: " + namespace);
        
        //switch on the namespace
        if(namespace == null)
        {
            log.error("WARNING: No namespace is declared.  Cannot parse this document.");
            return null;
        }
        else if(namespace.equals(
        		ObjectFormatCache.getInstance().getFormat("eml://ecoinformatics.org/eml-2.0.0").getFormatId().getValue()))
        {
            return parseEML200Document(d);
        }
        else if(namespace.equals(
        		ObjectFormatCache.getInstance().getFormat("eml://ecoinformatics.org/eml-2.0.1").getFormatId().getValue()))
        {
            return parseEML201Document(d);
        }
        else if(namespace.equals(
        		ObjectFormatCache.getInstance().getFormat("eml://ecoinformatics.org/eml-2.1.0").getFormatId().getValue()))
        {
            return parseEML210Document(d);
        }
        else if(namespace.equals(
        		ObjectFormatCache.getInstance().getFormat("eml://ecoinformatics.org/eml-2.1.1").getFormatId().getValue()))
        {
            return parseEML211Document(d);
        }
        else
        {
            throw new ParserConfigurationException(
                    "This parser only parses EML 2.0.0, 2.0.1 and 2.1.0.  Namespace " + 
                    namespace + " is not supported.");
        }
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
    
    /**
     * parse an EML 2.0.0 document
     * @param d
     * @return
     * @throws XPathExpressionException 
     * @throws NotFound 
     */
    private EMLDocument parseEML200Document(Document d) throws XPathExpressionException, NotFound
    {
        log.debug("Parsing an EML 2.0.0 document.");
        return parseEMLDocument(d, 
          ObjectFormatCache.getInstance().getFormat("eml://ecoinformatics.org/eml-2.0.0"));
    }
    
    /**
     * parse an EML 2.0.1 document
     * @param d
     * @return
     * @throws NotFound 
     */
    private EMLDocument parseEML201Document(Document d) 
      throws XPathExpressionException, NotFound
    {
        log.debug("Parsing an EML 2.0.1 document.");
        return parseEMLDocument(d, 
        	ObjectFormatCache.getInstance().getFormat("eml://ecoinformatics.org/eml-2.0.1"));
    }
    
    /**
     * parse and EML 2.1.0 document
     * @param d
     * @return
     * @throws NotFound 
     */
    private EMLDocument parseEML210Document(Document d) 
      throws XPathExpressionException, NotFound
    {
        log.debug("Parsing an EML 2.1.0 document.");
        return parseEMLDocument(d, 
        		ObjectFormatCache.getInstance().getFormat("eml://ecoinformatics.org/eml-2.1.0"));
    }
    
    /**
     * parse and EML 2.1.1 document
     * @param d
     * @return
     * @throws NotFound 
     */
    private EMLDocument parseEML211Document(Document d) 
      throws XPathExpressionException, NotFound
    {
        log.debug("Parsing an EML 2.1.1 document.");
        return parseEMLDocument(d, 
        		ObjectFormatCache.getInstance().getFormat("eml://ecoinformatics.org/eml-2.1.1"));
    }
    
    private EMLDocument parseEMLDocument(Document d, ObjectFormat docType) 
      throws XPathExpressionException, NotFound
    {
        log.debug("DataoneEMLParser.parseEMLDocument() called.");
        
        EMLDocument emld = new EMLDocument();
        NodeList result = runXPath("//distribution", d);        
        
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
                if(nl1.getLength() > 0)
                { //found a text format
                    log.debug("Found a text format");
                    mimeType = ObjectFormatCache.getInstance().getFormat("text/plain").toString();
                    NodeList nl4 = runXPath("dataFormat/textFormat/simpleDelimited", 
                                            physicalNode);
                    
                    // look for CSV files
                    if ( nl4.getLength() > 0 )
                    {
                      log.debug("Found a csv format");
                      mimeType = ObjectFormatCache.getInstance().getFormat("text/csv").toString();
                    }
                }
                else if(nl2.getLength() > 0)
                {
                    //TODO: could do a bit more parsing and refine this type more
                    log.debug("Found a binary raster format");
                    mimeType = ObjectFormatCache.getInstance().getFormat("application/octet-stream").toString();
                    
                }
                else if(nl3.getLength() > 0)
                {
                    // it's possible that the mime type is in this field.  
                    // Check for it and set the object format
                    log.debug("Found an externally defined format");
                    mimeType = ObjectFormatCache.getInstance().getFormat("application/octet-stream").toString();
                    NodeList nl5 = runXPath("dataFormat/externallyDefinedFormat/formatName", 
                                             physicalNode);
                    if ( nl5.getLength() > 0 )
                    {
                      Node childNode1 = nl5.item(0).getFirstChild();
                      String formatName = childNode1.getNodeValue();
                      
                      // set the object format if it exists
                      ObjectFormat format = ObjectFormatCache.getInstance().getFormat(formatName);
                      if ( !(format == null) )
                      {
                        mimeType = format.toString();
                      }
                    }
                    
                }

                log.debug("mime type: " + mimeType); 
                log.debug("url:       " + url);
                emld.addDistributionMetadata(url, mimeType);
            }
        }
        
        emld.setObjectFormat(docType);
        log.debug("EML document type: " + emld.format.toString());
        
        return emld;
    }
}
