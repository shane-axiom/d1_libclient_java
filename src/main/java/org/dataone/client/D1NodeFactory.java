package org.dataone.client;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.rest.DefaultHttpMultipartRestClient;
import org.dataone.client.rest.MultipartRestClient;
import org.dataone.client.v2.CNode;
import org.dataone.client.v2.MNode;
import org.dataone.client.v1.impl.MultipartCNode;
import org.dataone.client.v1.impl.MultipartMNode;
import org.dataone.client.v1.types.D1TypeBuilder;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Subject;
/**
 * D1NodeFactory contains methods for building CNode and MNode instances from
 * a URI.  In production context, the URI is a URL for the base service location
 * for that node (and begins with the scheme "http" or "https").  Alternate 
 * implementations are predominantly for testing, and can be built using the 
 * same notation, but with the scheme "java" to indicate building via Reflection.
 * 
 * Examples:
 *   D1NodeFactory.buildMNode("https://tla.institute.org/mn");
 *   D1NodeFactory.buildMNode("java:org.dataone.client.impl.rest.InMemoryMemberNode#Subject=mnAdmin&Subject=cnClient);
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

//	private MultipartRestClient restClient;
//	
//	public D1NodeFactory(MultipartRestClient mrc) {
//		this.restClient = mrc;
//	}
//	
//	public D1NodeFactory() {
//		this.restClient = new DefaultHttpMultipartRestClient();
//	}
//	
//	/**
//	 * Access the MultipartRestClient set by the parameterless constructor
//	 * @return
//	 */
//	public MultipartRestClient getRestClient() {
//		return this.restClient;
//	}
//	
//	/**
//	 * Instantiate a CNode instance of the right type from the given URI
//	 * @param uri
//	 * @return
//	 * @throws ClientSideException
//	 */
//	public CNode buildCNode(URI uri)
//	throws ClientSideException
//	{
//		return D1NodeFactory.buildCNode(restClient, uri);
//	}
//	
//	/**
//	 * Instantiate an MNode instance of the right type from the given URI
//	 * @param uri
//	 * @return
//	 * @throws ClientSideException
//	 */
//	public MNode buildMNode(URI uri)
//	throws ClientSideException
//	{
//		return D1NodeFactory.buildMNode(restClient, uri);
//	}
	
	/**
	 * Instantiate a CNode instance of the right type from the given URI
	 * and the provided MultipartRestClient
	 * @param mrc
	 * @param uri
	 * @return
	 * @throws ClientSideException
	 */
	public static org.dataone.client.v1.CNode build_v1_CNode(MultipartRestClient mrc, URI uri) 
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
	public static org.dataone.client.v1.MNode build_v1_MNode(MultipartRestClient mrc, URI uri) 
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
	public static org.dataone.client.v2.CNode build_v2_CNode(MultipartRestClient mrc, URI uri) 
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
	public static org.dataone.client.v2.MNode build_v2_MNode(MultipartRestClient mrc, URI uri) 
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
	

}
