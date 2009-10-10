// $Id$

package BFT.network;

import java.nio.channels.SocketChannel;


public class DataEvent {
	public SocketChannel socket;
	public byte[] data;
	
	public DataEvent(SocketChannel socket, byte[] data) {
		this.socket = socket;
		this.data = data;
	}
}