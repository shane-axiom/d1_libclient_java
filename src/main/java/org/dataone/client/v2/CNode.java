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
import org.dataone.service.cn.v2.CNAuthorization;
import org.dataone.service.cn.v2.CNCore;
import org.dataone.service.cn.v2.CNDiagnostic;
import org.dataone.service.cn.v2.CNIdentity;
import org.dataone.service.cn.v2.CNRead;
import org.dataone.service.cn.v2.CNRegister;
import org.dataone.service.cn.v2.CNReplication;
import org.dataone.service.cn.v2.CNView;

/**
 * An aggregated API that represents all of the possible services reachable at
 * a Coordinating Node, defined in the org.dataone.service.cn package.
 *
 * It also extends the D1Node interface which is used to associate the baseUrl
 * and NodeId / NodeReference.
 *
 * @author rnahf
 *
 */
public interface CNode
extends D1Node,
/* and all of the CN service interfaces */
CNCore, CNRead, CNAuthorization, CNIdentity, CNRegister, CNReplication, CNView, CNDiagnostic
{}
