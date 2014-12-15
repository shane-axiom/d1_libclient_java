package org.dataone.client;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeReference;

/**
 * Factory for creating new 
 * @author rnahf
 *
 */
public abstract class D1ServiceLocator {

	protected Class[] serviceHandlers;
	protected List<Node> nodes;
	protected Map<NodeReference, List<D1Node>> existingServiceInstances;
	
//	public D1ServiceLocator(D1Node[] serviceHandlers) {
//		this.serviceHandlers = serviceHandlers;
//	}
	
	
	public <T> T getService(NodeReference nodeRef, Class<T> serviceClass, ServiceValidator validator) {

		D1Node existingInstance = lookupExistingHandler(nodeRef, serviceClass);
		if (existingInstance != null) {
			return (T) existingInstance;
		}
		
		return null;
		
	}
	
	private D1Node locateHandler(Class serviceClass) {
		Class theHandler = null;
		for (Class handler : this.serviceHandlers) {
			if (serviceClass.isInterface()) {
				for (Class interphace: handler.getInterfaces()) {
					if (interphace.getCanonicalName().equals(serviceClass.getCanonicalName())) {
						theHandler = handler;
						break;
					}
				}
				
			}
			else {
				Class c = serviceClass;
				do {
					if (c.getCanonicalName().equals(handler.getCanonicalName())) {
						theHandler = handler;
						break;
					}
					c = c.getSuperclass();
				} 
				while (c != null);
			}
		}
		
		return null;
		
	}
	
	private D1Node lookupExistingHandler(NodeReference nodeRef, Class serviceClass) {
	
		List<D1Node> knownServiceInstances = this.existingServiceInstances.get(nodeRef);
		if (knownServiceInstances != null) {
			for(D1Node node : knownServiceInstances) {
				if (serviceClass.isInstance(node)) {
					return node;
				}
			}
		}
		return null;
	}
	
		

	

	public <T> T getService(URI uri, Class<T> serviceClass, ServiceValidator validator) {
		
		
		return null;
	}
}
