// $Id$




package BFT.network;


public interface Network {
    
 
    /**
       Send m[] to the node identified by role.index
     **/
    public void send(byte[] m, BFT.util.Role role, int index);

    /**
       Start listening for incoming messages
     **/
    public void start();

    /**
       Stop Listening for incoming messages
     **/
    public void stop();

}
