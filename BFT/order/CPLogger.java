/**
 * $Id$
 */
package BFT.order;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import BFT.order.statemanagement.*;

/**
 * @author riche
 *
 */
public class CPLogger implements Runnable {

	private CPQueue cpq = null;
	private CheckPointState cps = null;
	private Integer id = null;
	
	public CPLogger(CPQueue cpq, CheckPointState cps) {
		this.cpq = cpq;
		this.cps = cps;
	}
	
	public CPLogger(CPQueue cpq, CheckPointState cps, int id) {
		this(cpq, cps);
		this.id = new Integer(id);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
		
		String filename = null;
		if(id == null) {
			filename = cps.getCurrentSequenceNumber() + "_CP.LOG";
		}
		else {
			filename = cps.getCurrentSequenceNumber() + "_" + id + "_ORDER_CP.LOG";
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
		cps.makeStable();
		cpq.addWork(cps);
	}

}
