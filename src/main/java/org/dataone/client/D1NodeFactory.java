package org.dataone.client;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.client.v1.impl.MultipartCNode;
import org.dataone.client.v1.impl.MultipartMNode;
import org.dataone.client.v1.types.D1TypeBuilder;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Subject;

/**
 * D1NodeFactory contains a method for building CNode and MNode instances from
 * a URI.  In production context, the URI is a URL for the base service location
 * for that node (and begins with the scheme "http" or "https").  Alternate 
 * implementations are predominantly for testing, and can be built using the 
 * same notation, but with the scheme "java" to indicate building via Reflection.
 * 
 * Examples:
 *   D1NodeFactory.build_V2_MNode("https://tla.institute.org/mn");
 *   D1NodeFactory.build_V2_MNode("java:org.dataone.client.impl.rest.InMemoryMemberNode#Subject=mnAdmin&Subject=cnClient);
 * 
 * The first builds a standard MNode that communicates with the networked Member Node
 * The second instantiates an InMemoryMemberNode that takes the MemberNode admin and
 * CN client Subjects as parameters in the constructor.
 * 
 * The only parameters supported at this first implementation are:
 *  String, Integer, Identifier, NodeReference and Subject
 *  
 *  
 * @author rnahf
 *
 */
public class D1NodeFactory {

    /** map of service classes to {@link INodeCreator}s, which can create the concrete {@link D1Node} subclass */
    private static final Map<Class<?>, INodeCreator<?>> nodeCreatorMap = new HashMap<Class<?>, INodeCreator<?>>();
    static {
        // v1 CN
        registerService(org.dataone.service.cn.v1.CNCore.class, new V1CnBuilder());
        registerService(org.dataone.service.cn.v1.CNRead.class, new V1CnBuilder());
        registerService(org.dataone.service.cn.v1.CNAuthorization.class, new V1CnBuilder());
        registerService(org.dataone.service.cn.v1.CNIdentity.class, new V1CnBuilder());
        registerService(org.dataone.service.cn.v1.CNRegister.class, new V1CnBuilder());
        registerService(org.dataone.service.cn.v1.CNReplication.class, new V1CnBuilder());
        // v2 CN
        registerService(org.dataone.service.cn.v2.CNCore.class, new V2CnBuilder());
        registerService(org.dataone.service.cn.v2.CNRead.class, new V2CnBuilder());
        registerService(org.dataone.service.cn.v2.CNAuthorization.class, new V2CnBuilder());
        registerService(org.dataone.service.cn.v2.CNIdentity.class, new V2CnBuilder());
        registerService(org.dataone.service.cn.v2.CNRegister.class, new V2CnBuilder());
        registerService(org.dataone.service.cn.v2.CNReplication.class, new V2CnBuilder());
        // v1 MN
        registerService(org.dataone.service.mn.tier1.v1.MNCore.class, new V1MnBuilder());
        registerService(org.dataone.service.mn.tier1.v1.MNRead.class, new V1MnBuilder());
        registerService(org.dataone.service.mn.tier2.v1.MNAuthorization.class, new V1MnBuilder());
        registerService(org.dataone.service.mn.tier3.v1.MNStorage.class, new V1MnBuilder());
        registerService(org.dataone.service.mn.tier4.v1.MNReplication.class, new V1MnBuilder());
        registerService(org.dataone.service.mn.v1.MNQuery.class, new V1MnBuilder());
        // v2 MN
        registerService(org.dataone.service.mn.tier1.v2.MNCore.class, new V2MnBuilder());
        registerService(org.dataone.service.mn.tier1.v2.MNRead.class, new V2MnBuilder());
        registerService(org.dataone.service.mn.tier2.v2.MNAuthorization.class, new V2MnBuilder());
        registerService(org.dataone.service.mn.tier3.v2.MNStorage.class, new V2MnBuilder());
        registerService(org.dataone.service.mn.tier4.v2.MNReplication.class, new V2MnBuilder());
        registerService(org.dataone.service.mn.v2.MNQuery.class, new V2MnBuilder());
    }

    /**
     * Registers a service classes (an interface like MNRead, MNStorage, etc.) to an {@link INodeCreator} 
     * which can create the concrete {@link D1Node} subclass for the given service class.   
     * @param serviceClass 
     *      the class we want to get a node implementation for
     * @param nodeCreator 
     *      the {@link INodeCreator} responsible for producing a {@link D1Node} implementation
     */
    private static <N extends D1Node> void registerService(Class serviceClass, INodeCreator<N> nodeCreator) {
        nodeCreatorMap.put(serviceClass, nodeCreator);
    }
    
    /**
     * Creates a {@link D1Node} implementation for the given <code>serviceClass</code>.
     * 
     * @param serviceClass 
     *      the class representing the service we want to use on the node
     *      (MNRead, MNAuthorization, MNStorage, etc.)
     * @param mrc 
     *      the {@link MultipartRestClient} for the node REST requests
     * @param uri 
     *      the {@link URI} for the node being created
     * @return 
     *      a subclass of {@link D1Node}
     * @throws ClientSideException 
     *      if the {@link INodeCreator} fails to create the node
     */
    public static <N extends D1Node> N buildNode(Class<?> serviceClass, MultipartRestClient mrc, URI uri)
            throws ClientSideException {

        // safe cast, if we set up nodeCreatorMap and the creators correctly
        INodeCreator<N> nodeBuilder = (INodeCreator<N>) nodeCreatorMap.get(serviceClass);
        if (nodeBuilder != null)
            return nodeBuilder.buildNode(mrc, uri);

        throw new NotImplementedException("No INodeCreator exists for service class: "
                + serviceClass.getName());
    }

	/**
	 * Instantiate a CNode instance of the right type from the given URI
	 * and the provided MultipartRestClient
	 * @param mrc
	 * @param uri
	 * @return
	 * @throws ClientSideException
	 */
    private static org.dataone.client.v1.CNode build_v1_CNode(MultipartRestClient mrc, URI uri)
	throws ClientSideException 
	{
		// TOD check for nulls
		
		org.dataone.client.v1.CNode builtCNode = null;
		
		if (uri.getScheme() == null) {
			throw new ClientSideException("CN uri had no scheme");
		}
		if(uri.getScheme().equals("java")) {
			builtCNode = buildJavaD1Node(org.dataone.client.v1.CNode.class, uri);
		}
		else if (uri.getScheme().equals("http") || uri.getScheme().equals("https")) {
			// build the standard implementation
			builtCNode = new MultipartCNode( mrc, uri.toString());
		}
		
		return builtCNode;
	} 

	/**
	 * Instantiate an MNode instance of the right type from the given URI
	 * and the provided MultipartRestClient
	 * @param mrc
	 * @param uri
	 * @return
	 * @throws ClientSideException
	 */
    private static org.dataone.client.v1.MNode build_v1_MNode(MultipartRestClient mrc, URI uri)
	throws ClientSideException 
	{
		org.dataone.client.v1.MNode builtMNode = null;
		if(uri.getScheme().equals("java")) {
			builtMNode = buildJavaD1Node(org.dataone.client.v1.MNode.class, uri);
		}
		else if (uri.getScheme().equals("http") || uri.getScheme().equals("https")) {
			// build the standard implementation
			builtMNode = new MultipartMNode( mrc, uri.toString());
		} else {
			throw new ClientSideException("No corresponding builder for URI scheme: " + uri.getScheme());
		}
		
		return builtMNode;
	} 
	
	/**
	 * Instantiate a CNode instance of the right type from the given URI
	 * and the provided MultipartRestClient
	 * @param mrc
	 * @param uri
	 * @return
	 * @throws ClientSideException
	 */
    private static org.dataone.client.v2.CNode build_v2_CNode(MultipartRestClient mrc, URI uri)
	throws ClientSideException 
	{
		// TOD check for nulls
		
		org.dataone.client.v2.CNode builtCNode = null;
		
		if (uri.getScheme() == null) {
			throw new ClientSideException("CN uri had no scheme");
		}
		if(uri.getScheme().equals("java")) {
			builtCNode = buildJavaD1Node(org.dataone.client.v2.CNode.class, uri);
		}
		else if (uri.getScheme().equals("http") || uri.getScheme().equals("https")) {
			// build the standard implementation
			builtCNode = new org.dataone.client.v2.impl.MultipartCNode( mrc, uri.toString());
		}
		
		return builtCNode;
	} 

	/**
	 * Instantiate an MNode instance of the right type from the given URI
	 * and the provided MultipartRestClient
	 * @param mrc
	 * @param uri
	 * @return
	 * @throws ClientSideException
	 */
    private static org.dataone.client.v2.MNode build_v2_MNode(MultipartRestClient mrc, URI uri)
	throws ClientSideException 
	{
		org.dataone.client.v2.MNode builtMNode = null;
		if(uri.getScheme().equals("java")) {
			builtMNode = buildJavaD1Node(org.dataone.client.v2.MNode.class, uri);
		}
		else if (uri.getScheme().equals("http") || uri.getScheme().equals("https")) {
			// build the standard implementation
			builtMNode = new org.dataone.client.v2.impl.MultipartMNode( mrc, uri.toString());
		} else {
			throw new ClientSideException("No corresponding builder for URI scheme: " + uri.getScheme());
		}
		
		return builtMNode;
	} 
	
	/**
	 * This function instantiates a CNode or MNode implementation via reflection
	 * from the information given in the URI.  The name of the class to instantiate
	 * is given in the scheme-specific part, and parameters for the constructor
	 * in the URI fragment.  java:fully.qualified.className#String=name&Subject=admin 
	 */
	//TODO: find different separator for fragment, because '=' is in most Subject strings
	//  don't use ':' because colon in NodeReference string, maybe '#' will work or '/'?
	// or make it the first (non alpha) character encountered.	
	@SuppressWarnings("rawtypes")
	private static <T> T buildJavaD1Node(Class<T> domainClass, URI uri) throws ClientSideException 
	{
		try {
			String frag = uri.getFragment();
			String[] kvPairs = StringUtils.split(frag,"&");
			Class[] constructorParamTypes = new Class[kvPairs.length];
			Object[] initargs = new Object[kvPairs.length];
			for (int i=0;i<kvPairs.length; i++) {
				String[] pair = StringUtils.split(kvPairs[i], "=");
				if (pair[0].equals("Identifier")) {
					constructorParamTypes[i] = Identifier.class;
					initargs[i] = D1TypeBuilder.buildIdentifier(pair[1]);
				} else if (pair[0].equals("Subject")) {
					constructorParamTypes[i] = Subject.class;
					initargs[i] = D1TypeBuilder.buildSubject(pair[1]);
				} else if (pair[0].equals("NodeReference")) {
					constructorParamTypes[i] = NodeReference.class;
					initargs[i] = D1TypeBuilder.buildNodeReference(pair[1]);
				} else if (pair[0].equals("String")) {
					constructorParamTypes[i] = String.class;
					initargs[i] = pair[1];
				} else if (pair[0].equals("Integer")) {
					constructorParamTypes[i] = Integer.class;
					initargs[i] = new Integer(pair[1]);
				} else {
					throw new ClientSideException("Malformed fragment in nodeBaseUrl to form constructor arguments");
				}
			}
			Constructor c = Class.forName(uri.getSchemeSpecificPart()).getConstructor(constructorParamTypes);
			return (T) c.newInstance(initargs);
		} catch (SecurityException e) {
			throw new ClientSideException("Error in buildJavaMNodeFromURI",e);
		} catch (NoSuchMethodException e) {
			throw new ClientSideException("Error in buildJavaMNodeFromURI",e);
		} catch (ClassNotFoundException e) {
			throw new ClientSideException("Error in buildJavaMNodeFromURI",e);
		} catch (IllegalArgumentException e) {
			throw new ClientSideException("Error in buildJavaMNodeFromURI",e);
		} catch (InstantiationException e) {
			throw new ClientSideException("Error in buildJavaMNodeFromURI",e);
		} catch (IllegalAccessException e) {
			throw new ClientSideException("Error in buildJavaMNodeFromURI",e);
		} catch (InvocationTargetException e) {
			throw new ClientSideException("Error in buildJavaMNodeFromURI",e);
		} 
	}
	

    /**
     * The interface for all the node creator classes. Just requires implementors to provide one method:  
     * {@link INodeCreator#buildNode(MultipartRestClient, URI)}.
     * 
     * @param <N> the type of node the creator will produce. Must be a subtype of {@link D1Node}.
     */
    private static interface INodeCreator<N extends D1Node> {
        /**
         * Creates a type of {@link D1Node} based on {@link INodeCreator}'s parameter type <code>&lt;N&gt;</code>
         * 
         * @param mrc 
         *      the {@link MultipartRestClient} for the node REST requests
         * @param uri 
         *      the {@link URI} for the node being created
         * @return 
         *      a subclass of {@link D1Node}. Type depends on <code>&lt;N&gt;</code>.
         *      
         * @throws ClientSideException 
         *      if the {@link INodeCreator} fails to create the node
         */
        public N buildNode(MultipartRestClient mrc, URI uri) throws ClientSideException;
    }

    /**
     * Creates a {@link org.dataone.client.v1.CNode}
     */
    private static class V1CnBuilder implements INodeCreator<org.dataone.client.v1.CNode> {
        @Override
        public org.dataone.client.v1.CNode buildNode(MultipartRestClient mrc, URI uri) throws ClientSideException {
            return build_v1_CNode(mrc, uri);
        }
    }

    /**
     * Creates a {@link org.dataone.client.v2.CNode}
     */
    private static class V2CnBuilder implements INodeCreator<org.dataone.client.v2.CNode> {
        @Override
        public org.dataone.client.v2.CNode buildNode(MultipartRestClient mrc, URI uri) throws ClientSideException {
            return build_v2_CNode(mrc, uri);
        }
    }

    /**
     * Creates a {@link org.dataone.client.v1.MNode}
     */
    private static class V1MnBuilder implements INodeCreator<org.dataone.client.v1.MNode> {
        @Override
        public org.dataone.client.v1.MNode buildNode(MultipartRestClient mrc, URI uri) throws ClientSideException {
            return build_v1_MNode(mrc, uri);
        }
    }

    /**
     * Creates a {@link org.dataone.client.v2.MNode}
     */
    private static class V2MnBuilder implements INodeCreator<org.dataone.client.v2.MNode> {
        @Override
        public org.dataone.client.v2.MNode buildNode(MultipartRestClient mrc, URI uri) throws ClientSideException {
            return build_v2_MNode(mrc, uri);
        }
    }
}
