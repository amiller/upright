// $Id$

package BFT.network;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.Selector;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.net.Socket;
import java.net.ServerSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.nio.channels.SelectionKey;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;


public class Worker implements Runnable {
    private List<DataEvent> queue = new LinkedList<DataEvent>();
    protected TCPNetwork net;
    
    public Worker(TCPNetwork n){
	this.net = n;
	net.setWorker(this);
    }
  
  public void processData(SocketChannel socket, byte[] data, int count) {
    byte[] dataCopy = new byte[count];
    System.arraycopy(data, 0, dataCopy, 0, count);
    synchronized(queue) {
      queue.add(new DataEvent(socket, dataCopy));
      queue.notify();
    }
  }
  
  public void run() {
    DataEvent dataEvent;
    
    while(true) {
      // Wait for data to become available
      synchronized(queue) {
        while(queue.isEmpty()) {
          try {
            queue.wait();
          } catch (InterruptedException e) {
          }
        }
        dataEvent = queue.remove(0);
      }
      
      net.handleRawBytes(dataEvent.socket, dataEvent.data);
      // Return to sender
      //this.baseNode.send(dataEvent.socket, dataEvent.data);
    }
  }
} 



