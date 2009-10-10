// $Id$

/**
 * 
 */
package BFT.order;

import java.util.concurrent.*;
import java.util.*;
import BFT.membership.*;
import BFT.messages.ClientRequest;
import BFT.messages.VerifiedMessageBase;
import BFT.order.MessageFactory;
import BFT.messages.*;
import BFT.util.*;
import BFT.Parameters;
import BFT.order.statemanagement.RequestQueue;
import BFT.network.concurrentNet.*;

/**
 * @author riche
 *
 */
public interface OrderNetworkWorkQueue extends NetQueue{
//extends BFT.network.concurrentNet.NetworkWorkQueue{
//
//	RequestQueue[] filteredWorkQueue;
//	private int filterIndex = 0;
//
//
//	public OrderNetworkWorkQueue() {
//		super();
//		filteredWorkQueue = new RequestQueue[Parameters.getFilterCount()];
//		for (int i = 0; i < filteredWorkQueue.length; i++)
//			filteredWorkQueue[i] = new RequestQueue();
//	}
//
//	public void addFilteredWork(int index, FilteredRequest req){
//		this.announceNetWork(Role.FILTER);
//		if (!filteredWorkQueue[index].add(req)) this.tookNetWork(Role.FILTER);
//	}
//
	public FilteredRequestCore getFilteredWork2(int index);
//	FilteredRequest req = (FilteredRequest) filteredWorkQueue[index].poll();
//	if (req != null) this.tookNetWork(Role.FILTER);
//	return req;
//}
//
public FilteredRequestCore getFilteredWorkRR2();
//		int index = filterIndex;
//		FilteredRequest retBytes = getFilteredWork(filterIndex);
//		filterIndex++;
//		if (filterIndex >= BFT.Parameters.getFilterCount()) filterIndex = 0;
//		while (retBytes == null && filterIndex != index){
//			retBytes = getFilteredWork(filterIndex);
//			filterIndex++;
//			if(filterIndex >= BFT.Parameters.getFilterCount()) {
//				filterIndex = 0;
//			}
//		}
//		return retBytes;
//	}

}
