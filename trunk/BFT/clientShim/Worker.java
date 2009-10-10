/**
 * $Id$
 */
package BFT.clientShim;

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
 * @author riche
 *
 */
public class Worker implements Runnable {

	NetworkWorkQueue netQueue = null;
	ClientShimBaseNode protocolHandler = null;

	public Worker(NetworkWorkQueue netQueue, ClientShimBaseNode protocolHandler) {
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
		while(true) {
			netQueue.hasAnyNetWork();
			if (netQueue.hasNetWork(Role.EXEC))
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
