// $Id$

package BFT.network.concurrentNet;

import java.util.*;
import BFT.util.*;

public interface ConcurrentNetwork {

	public void send(byte[] m, int index);
	public void start();
	public void stop();
	public void waitForListening();
	public Queue<Pair<Integer, byte []>> select();
	public Role getMyRole();
}
