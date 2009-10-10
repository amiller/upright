/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package BFT.util;

import java.util.Hashtable;
import java.net.Socket;

/**
 *
 * @author manos
 */
public class Multiplexor {
    Hashtable<Socket, Integer> sockets;

    public Multiplexor() {
        sockets = new Hashtable<Socket, Integer>();
    }
    
    public synchronized void add(Socket socket) {
        sockets.put(socket, 0);
    }
    
    public Hashtable<Socket, Integer> getSockets() {
        return sockets;
    }
    
}
