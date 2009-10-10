/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package BFT.network.concurrentNet;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import BFT.util.Role;

/**
 *
 * @author manos
 */
public class BFTPipelineFactory implements ChannelPipelineFactory {
    
    Role role;
    int id;
    NetworkWorkQueue NWQ;
    
    public BFTPipelineFactory(Role _role, int _id, NetworkWorkQueue nwq) {
        role = _role;
        id = _id;
        NWQ = nwq;
    }
    
    public ChannelPipeline getPipeline() {
        ChannelPipeline pipeline = Channels.pipeline();
        ServerHandler handler = new ServerHandler(role, id, NWQ);
		MessageDecoder decoder = new MessageDecoder(role , id);
        pipeline.addLast("decoder", decoder);
        pipeline.addLast("handler", handler);
        return pipeline;
    }


}
