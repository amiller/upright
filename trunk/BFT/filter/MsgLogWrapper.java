/**
 * $Id$
 */
package BFT.filter;

import BFT.messages.*;

/**
 * @author riche
 *
 */
public class MsgLogWrapper {
	
	private LogFileOps op;
	private FilteredRequestCore frc = null;
	private Entry entry = null;
	
	
	
    public MsgLogWrapper(FilteredRequestCore nb, Entry entry) {
		this.frc = nb;
		this.entry = entry;
	}
	public MsgLogWrapper(Entry ent) {
		this.entry = ent;
	}
	
	public MsgLogWrapper(LogFileOps op) {
		this.op = op;
		this.frc = null;
	}

	/**
	 * @return the nb
	 */
	public FilteredRequestCore getCore() {
		return frc;
	}

	/**
	 * @return the op
	 */
	public LogFileOps getOp() {
		return op;
	}

	/**
	 * @return the entry
	 */
	public Entry getEntry() {
		return entry;
	}

	
}
