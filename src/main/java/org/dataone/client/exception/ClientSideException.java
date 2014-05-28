package org.dataone.client.exception;

public class ClientSideException extends Exception{

	   /**
     * Construct a NotCached exception with the message.
     * 
     * @param message the description of this exception
     */
    public ClientSideException(String message, Throwable cause) {
        super(message);
        this.initCause(cause);
    }
    
}
