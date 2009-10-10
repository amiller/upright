package BFT.network.concurrentNet;

import BFT.util.*;

import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channel;

public class MessageDecoder extends FrameDecoder {

    Role role;
    int id;
    
    public MessageDecoder(Role _role, int _id) {
        role = _role;
        id = _id;
    }
    
    @Override
    protected Object decode(
            ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) {
        
        if(role == Role.CLIENT && id == 0) {
         ////System.out.println("I got a message with "+buffer.readableBytes()+" readable bytes starting at "+buffer.readerIndex());
        }
        
    	if(buffer.readableBytes() < 4) { // we still dont know the length
    	    return null;
    	} else {
    		byte[] lenBytes = new byte[4];
    	    
    		//buffer.getBytes(buffer.readerIndex(), lenBytes, 0, 4);
            buffer.markReaderIndex();
            buffer.readBytes(lenBytes, 0, 4);
    if(role == Role.CLIENT && id == 0) {
    ////System.out.print("bytes 1: ");            BFT.util.UnsignedTypes.printBytes(lenBytes);
    }
    	    int len = (int)UnsignedTypes.bytesToLong(lenBytes);
    	    if(buffer.readableBytes() < len+4) {
                //byte[] tempBytes = new byte[buffer.readableBytes()];
                //buffer.getBytes(buffer.)
                if(role == Role.CLIENT && id == 0) {
                ////System.out.println("Did not find a full message");
                }
                buffer.resetReaderIndex();
                ////System.out.println("readerIndex is reset to: "+buffer.readerIndex());
                return null;
    	    } else {
    	    	byte[] messageBytes = new byte[len];
    	    	//I am just skipping the length while reading, I already have it 
//                buffer.skipBytes(4);
                //buffer.readBytes(lenBytes, 0, 4);
                if(role == Role.CLIENT && id == 0) {
                ////System.out.print("Bytes 2: ");  BFT.util.UnsignedTypes.printBytes(lenBytes);
                ////System.out.println();
                } 
    	    	buffer.readBytes(messageBytes,0,len);
    	    	//I can reuse the lenBytes buffer here to read the marker
    	    	buffer.readBytes(lenBytes,0,4);
    	    	int marker = (int)UnsignedTypes.bytesToLong(lenBytes);
    	    	if(marker != len) {
                    BFT.util.UnsignedTypes.printBytes(messageBytes);
                    System.out.println();
                    BFT.util.UnsignedTypes.printBytes(lenBytes);
                    System.out.println();
                    BFT.Debug.kill(new RuntimeException("invalid marker "+len + " "+marker+" from "+role+"."+id));
                }
    	    	
    	    	Pair<Integer, byte[]> parsedMsgBytes = new Pair<Integer, byte[]>(0, messageBytes);
                if(role == Role.CLIENT && id == 0) {
                ////System.out.println("I am returning "+messageBytes.length+" bytes to the handler finishing at "+buffer.readerIndex());
                }
    	    	return parsedMsgBytes;
    	    }
    	}
        /*if (buffer.readableBytes() < 4) {
            return null;
        }
        
        return buffer.readBytes(4);*/
    }
}