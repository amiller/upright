/**
 * $Id$
 */
package BFT.network.concurrentNet;

import BFT.order.*;

/**
 * @author riche
 *
 */
public class ReadPredicate {
	
	private OrderBaseNode obn;
	private OrderWorkQueue omq;
	
	public ReadPredicate(OrderWorkQueue omq, OrderBaseNode obn) {
		this.obn = obn;
		this.omq = omq;
	}

	public boolean predicate(RPChooser rc) {
		boolean retval = false;
		switch(rc) {
		case CLEAN_RC: 
		    retval = obn.canCreateBatch(); break;
		default: retval = true; break;
		}
		return retval;
	}
	
}
