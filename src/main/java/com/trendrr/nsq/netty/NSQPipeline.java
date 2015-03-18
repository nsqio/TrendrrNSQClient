package com.trendrr.nsq.netty;
/**
 *
 */


import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 *
 */
public class NSQPipeline implements ChannelPipelineFactory {

    protected static Logger log = LoggerFactory.getLogger(NSQPipeline.class);

    public ChannelPipeline getPipeline() throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("codec", new NSQDecoder());

        pipeline.addLast("handler", new NSQHandler());

        pipeline.addLast("encoder", new NSQEncoder());
        return pipeline;
    }

}
