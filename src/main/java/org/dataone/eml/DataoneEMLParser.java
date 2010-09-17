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
import java.util.Vector;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.dataone.service.types.ObjectFormat;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * @author berkley
 * This class parses EML documents to pull any relevant D1 information
 * for systemMetadata creation and other purposes.
 */
public class DataoneEMLParser
{
    private static DataoneEMLParser parser = null;
    
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
     */
    public EMLDocument parseDocument(InputStream is)
        throws ParserConfigurationException, IOException, SAXException, XPathExpressionException
    {
        //info we need:
        //1) any distribution urls
        //2) doctype (public_id)
        //3) mime type for each 1
        EMLDocument doc = new EMLDocument();
        
        System.out.println("parsing EML document for any distribution urls");
        DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
        builderFactory.setNamespaceAware(true);
        
        Document d = builderFactory.newDocumentBuilder().parse(is);
        String namespace = d.getFirstChild().getNamespaceURI();
        System.out.println("namespace: " + namespace);
        
        if(namespace.equals(ObjectFormat.EML_2_0_0.toString()))
        {
            return parseEML200Document(d);
        }
        
        XPathFactory factory = XPathFactory.newInstance();
        XPath xpath = factory.newXPath();
        /*XPathExpression expr = xpath.compile("//distribution/online/url");
        org.w3c.dom.NodeList result = (org.w3c.dom.NodeList)expr.evaluate(d, XPathConstants.NODESET);
        
        System.out.println("result nodelist contains " + result.getLength() + " nodes");
        
        for (int i = 0; i < result.getLength(); i++) 
        {
            //System.out.println("result node name: " + result.item(i).getNodeName());
            System.out.println("found url: " + result.item(i).getFirstChild().getNodeValue()); 
            String nodeName = result.item(i).getNodeName();
            String nodeVal = result.item(i).getFirstChild().getNodeValue();
            if(nodeName.equals("url"))
            {
                urls.add(nodeVal);
            }
        }*/
        System.out.println("done parsing EML document");
        return doc;
    }
    
    private EMLDocument parseEML200Document(Document d)
    {
        EMLDocument emld = new EMLDocument();
        System.out.println("Parsing an EML 2.0.0 document.");
        return emld;
    }
    
    private EMLDocument parseEML201Document(Document d)
    {
        EMLDocument emld = new EMLDocument();
        return emld;
    }
    
    private EMLDocument parseEML210Document(Document d)
    {
        EMLDocument emld = new EMLDocument();
        return emld;
    }
}
