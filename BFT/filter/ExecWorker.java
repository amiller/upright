/**
 * $Id$
 */
package BFT.filter;

import java.util.*;


import BFT.Debug;
import BFT.order.*;
import BFT.messages.*;
import BFT.network.MessageHandler;
import BFT.network.concurrentNet.NetworkWorkQueue;
import BFT.network.concurrentNet.RPChooser;
import BFT.network.concurrentNet.ReadPredicate;
import BFT.util.*;
import BFT.order.statemanagement.*;

/**
 * @author aclement
 *
 */
public class ExecWorker implements Runnable {

	NetworkWorkQueue netQueue = null;
    FilterBaseNode protocolHandler = null;

	public ExecWorker(NetworkWorkQueue netQueue, FilterBaseNode protocolHandler) {
		this.netQueue = netQueue;
		this.protocolHandler = protocolHandler;
	}

    /**
       Can probably remove the vast majority of the listeners below
       --- we only care about the server here !
    **/

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
		byte[] bytesRead = null;
		int count = 0;
		while(true) {
			netQueue.hasAnyNetWork();
			bytesRead = netQueue.getWorkRR(Role.EXEC);
			if (bytesRead != null) {
			    Debug.profileStart("EXEC_BYTES");
			    protocolHandler.handle(bytesRead);
			    Debug.profileFinis("EXEC_BYTES");
			    bytesRead = null;
			    //continue;
			}
		}
	}

}
