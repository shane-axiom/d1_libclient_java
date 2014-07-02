package org.dataone.client.exception;

/**
 * A Client-Side exception is a wrapper for any implementation-specific exception
 * thrown by external packages.  This exception can be reached via getCause()
 * 
 * @author rnahf
 *
 */
public class ClientSideException extends Exception {

	   /**
	 * 
	 */
	private static final long serialVersionUID = -4019335773320078115L;

	public ClientSideException(String message) {
		super(message);
	}

	
	/**
     * Construct a ClientSideException  with the message.
     * 
     * @param message the description of this exception
     */
    public ClientSideException(String message, Throwable cause) {
        super(concatMessage(message, cause));
        this.initCause(cause);
    }
 
    private static String concatMessage(String message, Throwable cause) {
    	if (cause != null) {
    		return message + "/" + cause.getMessage();
    	}
    	return message;
    }
    
 
}
