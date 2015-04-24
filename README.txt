====
    This work was created by participants in the DataONE project, and is
    jointly copyrighted by participating institutions in DataONE. For
    more information on DataONE, see our web site at http://dataone.org.

      Copyright ${year}

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

    $Id$
====

DataONE Java Client Library
---------------------------

d1_libclient_java is a client library for calling DataONE services. The library
exposes the DataONE services as a set of Java classes and method calls, such that
the calling application does not have to have a complete understanding of the
REST API, and need only think only in terms of Java classes and interfaces when
making the actual calls.

Within each API method implementation, d1_libclient_java handles the security, serialization
of requests, deserialization of responses, proper triggering of exceptions, and
optional caching behavior.

In this way the library makes it easy to utilize DataONE services without having 
to have a complete understanding of the REST API.

Application developers should be aware of configuration options encapsulated in
property files contained within the .jar.  Security details and options are discussed
in the javadoc for org.dataone.client.auth.CertificateManager, and connection
timeout settings in org.dataone.client.D1Node.

Use of Apache's HttpClient figures prominently in the actual understanding of how
http calls are made, and the Foresite library for creation of ResourceMaps.

See the test classes under src/test for example usage.

See LICENSE.txt for the details of distributing this software. 
