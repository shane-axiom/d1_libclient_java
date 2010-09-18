package org.dataone.client;

/**
 * CNode represents a DataONE Coordinating Node, and allows calling classes to
 * execute CN services.
 */
public class CNode extends D1Node {

    /**
     * Construct a Coordinating Node, passing in the base url for node services.
     * @param nodeBaseServiceUrl base url for constructing service endpoints.
     */
    public CNode(String nodeBaseServiceUrl) {
        super(nodeBaseServiceUrl);
    }

}
