/**
 * $Id$
 */
package BFT.serverShim;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import BFT.serverShim.statemanagement.CheckPointState;

/**
 * @author riche
 *
 */
public class CPLogger implements Runnable {

	private ShimBaseNode cpq = null;
	private CheckPointState cps = null;
	private Integer id = null;
	
	public CPLogger( ShimBaseNode cpq, CheckPointState cps) {
		this.cps = cps;
		this.cpq = cpq;
	}
	
	public CPLogger(ShimBaseNode cpq, CheckPointState cps, int id) {
		this(cpq, cps);
		this.id = new Integer(id);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
		
		String filename = null;
		if(id == null) {
			filename = cps.getMaxSequenceNumber() + "_SHIM_CP.LOG";
		}
		else {
			filename = cps.getMaxSequenceNumber() + "_" + id + "_SHIM_CP.LOG";
		}
		try {
			//BFT.//Debug.println("LOGGING CP to " + filename);
			FileOutputStream out = new FileOutputStream(filename);

			out.write(cps.getBytes());
			out.close();
		} catch (FileNotFoundException e) {
			BFT.Debug.kill(e);
		} catch (IOException e) {
			BFT.Debug.kill(e);
		}
		cps.markStable();
		cpq.makeCpStable(cps);
	}

}
