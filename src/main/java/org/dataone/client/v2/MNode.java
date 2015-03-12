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
 */
package org.dataone.client.v2;

import org.dataone.client.D1Node;
import org.dataone.service.mn.tier1.v2.MNCore;
import org.dataone.service.mn.tier1.v2.MNRead;
import org.dataone.service.mn.tier2.v2.MNAuthorization;
import org.dataone.service.mn.tier3.v2.MNStorage;
import org.dataone.service.mn.tier4.v2.MNReplication;
import org.dataone.service.mn.v2.MNPackage;
import org.dataone.service.mn.v2.MNQuery;
import org.dataone.service.mn.v2.MNView;


/**
 * An aggregated API that represents all of the possible services reachable at 
 * a Member Node, defined in the org.dataone.service.mn package.  
 * 
 * It also extends the D1Node interface which is used to associate the baseUrl
 * and NodeId / NodeReference.
 * 
 * @author rnahf
 *
 */
public interface MNode 
extends D1Node,
/* and all of the MN service interfaces */
MNCore, MNRead, MNAuthorization, MNStorage, MNReplication, MNQuery, MNView, MNPackage
{}
