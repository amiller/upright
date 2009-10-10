package BFT.network.concurrentNet;

import BFT.util.Role;
import BFT.util.Pair;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
  
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
 
/**
* Handler implementation for the server.
*
* @author The Netty Project (netty-dev@lists.jboss.org)
* @author Trustin Lee (tlee@redhat.com)
*
* @version $Rev: 1685 $, $Date: 2009-08-28 16:15:49 +0900 (Fri, 28 Aug 2009) $
*/
@ChannelPipelineCoverage("one")
public class ServerHandler extends SimpleChannelUpstreamHandler {

    Role role;
    int id;
    NetworkWorkQueue NWQ;
    
    public ServerHandler(Role _role, int _id, NetworkWorkQueue _NWQ) {
        role = _role;
        id = _id;
        NWQ = _NWQ;
    }
    
    private static final Logger logger = Logger.getLogger(
            ServerHandler.class.getName());

    private final AtomicLong transferredBytes = new AtomicLong();

    public long getTransferredBytes() {
        return transferredBytes.get();
    }

    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) {
        // Send back the received message to the remote peer.
        //transferredBytes.addAndGet(((ChannelBuffer) e.getMessage()).readableBytes());
        //e.getChannel().write(e.getMessage());
    	Pair<Integer, byte[]> pair = (Pair<Integer, byte[]>)e.getMessage();
        byte[] bytes = pair.getRight();
        ////System.out.println("I got "+bytes.length+" bytes from the decoder. Role = "+role+" id="+id);
    	NWQ.addWork(role, id, pair.getRight());
    }

    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx, ExceptionEvent e) {
        // Close the connection when an exception is raised.
        logger.log(
                Level.WARNING,
                "Unexpected exception from downstream.",
                e.getCause());
        e.getChannel().close();
    }
}
