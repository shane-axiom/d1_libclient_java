package org.dataone.client;

import static org.junit.Assert.*;

import java.net.URI;

import org.dataone.client.exception.ClientSideException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class D1NodeFactoryTest {

    @Before
    public void setUp() throws Exception
    {
    }

    @After
    public void tearDown() throws Exception
    {
    }

    @Test
    public void testBuildNode_javaNodes() throws ClientSideException
    {
        org.dataone.client.v2.MNode authMNode =
                D1NodeFactory.buildNode(org.dataone.client.v2.MNode.class, null,
                URI.create("java:org.dataone.client.v2.impl.InMemoryMNode#Subject=authMnAdmin&Subject=authMnClient&NodeReference=urnnodetheCN"));
    }

}
