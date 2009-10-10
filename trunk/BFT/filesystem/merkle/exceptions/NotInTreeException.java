/**
 * $Id$
 */
package BFT.filesystem.merkle.exceptions;

/**
 * @author riche
 *
 */
public class NotInTreeException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4476864098075700174L;

	/**
	 * 
	 */
	public NotInTreeException() {
		super();
	}

	/**
	 * @param message
	 */
	public NotInTreeException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public NotInTreeException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public NotInTreeException(String message, Throwable cause) {
		super(message, cause);
	}

}
