// $Id$

package org.apache.zookeeper.server;

public class SessionInfo {
	public int timeout;
	public long timeToTimeout;
	
	public SessionInfo(int timeout, long timeToTimeout){
		this.timeout=timeout;
		this.timeToTimeout=timeToTimeout;
	}
}
