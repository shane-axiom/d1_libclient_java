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

import java.util.Vector;

import org.dataone.service.types.ObjectFormat;

/**
 * @author berkley
 * This class encapsulates any information garnered from an EML document
 * from the DataoneEMLParser
 */
public class EMLDocument
{
    public ObjectFormat format = null;
    public Vector<DistributionMetadata> distributionMetadata = null;
    
    /**
     * constructor
     */
    public EMLDocument()
    {
        distributionMetadata = new Vector<DistributionMetadata>();
    }
    
    /**
     * add a url and mime type to the document reference
     * @param url
     * @param mimeType
     */
    public void addDistributionMetadata(String url, String mimeType)
    {
        distributionMetadata.add(new DistributionMetadata(url, mimeType));
    }
    
    /**
     * set the format of the document
     * @param format
     */
    public void setObjectFormat(ObjectFormat format)
    {
        this.format = format;
    }
    
    /**
     * sub class to contain the distribution metadata
     * @author berkley
     *
     */
    public class DistributionMetadata
    {
        public DistributionMetadata(String url, String mimeType)
        {
            this.url = url;
            this.mimeType = mimeType;
        }
        
        public DistributionMetadata()
        {
            this.url = null;
            this.mimeType = null;
        }
        
        public String url;
        public String mimeType;
    }
}
