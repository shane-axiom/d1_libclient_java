package org.dataone.client;


/**
 * ServiceValidator implementations validate that a
 * DataONE service exists.  
 * @author rnahf
 *
 */
public interface ServiceValidator {

	public boolean validate();
	
}
