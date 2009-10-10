// $Id$

package BFT.network;

import BFT.BaseNode;

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

public class Listener implements Runnable {


    TCPNetwork network;
    public Listener(TCPNetwork net){
        network = net;
    }

    public void run() {
        // selector loop
        while (true) {
            try {
                if(network.listening) {
                    // Process any pending changes
                    synchronized(network.changeRequests) {
                        Iterator changes = network.changeRequests.iterator();
                        while (changes.hasNext()) {
                            ////System.out.println("Handling changeRequest");
                            ChangeRequest change = (ChangeRequest) changes.next();
                            switch (change.type) {
			    case ChangeRequest.CHANGEOPS:
				////System.out.println("Handling CHANGEOPS: "+change.ops);
				SelectionKey key = change.socket.keyFor(network.selector);
				////System.out.println("After assigning key");

				if(!key.isValid()) {
				    continue;
				}
                                  
				//if(key != null) {
				key.interestOps(change.ops);
				//} else {
				//    //System.out.println("the key is NULL and op="+change.ops);
				//    System.exit(0);
				//}
				////System.out.println("After assigning op");
				break;
			    case ChangeRequest.REGISTER:
				////System.out.println("Handling REGISTER");
				change.socket.register(network.selector, change.ops);
				SelectionKey k = change.socket.keyFor(network.selector);
				if(k == null) {
				    ////System.out.println("hmm null");
				} else {
				    ////System.out.println("hmm NOT null");
				}
				break;
                            }
			}
			////System.out.println("clearing requests");
			network.changeRequests.clear();
                    }

                    //////System.out.println("Before select");
                    // Wait for an event one of the registered channels
                    network.selector.select();

                    // Iterate over the set of keys for which events are available
                    Iterator<SelectionKey> selectedKeys = network.selector.selectedKeys().iterator();
                    while (selectedKeys.hasNext()) {
                        SelectionKey key = selectedKeys.next();
                        selectedKeys.remove();

                        if (!key.isValid()) {
                            //System.out.println("Invalid key in selection");
                            continue;
                        }
                        try {
                            // Check what event is available and deal with it
                            if (key.isAcceptable()) {
                                //////System.out.println("Acceptable socketChannel");
                                network.accept(key);
                            } else if (key.isReadable()) {
                                //////System.out.println("Readable socketChannel");
                                network.read(key);
                            } else if (key.isWritable()) {
                                //////System.out.println("writable socketChannel");
                                network.write(key);
                            } else if (key.isConnectable()) {
                                //////System.out.println("connectable socketChannel");
                                network.finishConnection(key);
                            } else {
                                //System.out.println("Something else");
                            }
                        } catch (IOException ioe) {
                            for (Enumeration<String> e = network.sockets.keys(); e.hasMoreElements();)
				{
				    String stringKey = e.nextElement();
				    SocketChannel sc = network.sockets.get(stringKey);
				    if(sc.equals( (SocketChannel)key.channel()) ) {
					network.sockets.remove(stringKey);
					break;
				    }
				}
                            key.cancel();
                        }
                    }
                } else {
                    //wait a while before checking if listening is true
                    Thread.sleep(100);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}