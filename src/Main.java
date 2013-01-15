/**
 * 
 */

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import commands.NSQCommand;


/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 * 
 */
public class Main {

	protected static Log log = LogFactory.getLog(Main.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
		
		
		
		
		//example lookup (works)
//		NSQLookup lookup = new NSQLookup();
//		lookup.addAddr("localhost", 4161);
//		System.out.println(lookup.lookup("test"));
		
		
		//netty connection 
		
		// Configure the client.
        ClientBootstrap bootstrap = new ClientBootstrap(
                new NioClientSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory(new NSQPipeline());

        // Start the connection attempt.
        ChannelFuture future = bootstrap.connect(new InetSocketAddress("localhost", 4150));

        // Wait until the connection attempt succeeds or fails.
        Channel channel = future.awaitUninterruptibly().getChannel();
        if (!future.isSuccess()) {
            future.getCause().printStackTrace();
            bootstrap.releaseExternalResources();
            return;
        }
        
        
        Connection conn = new Connection(channel);
        
        //identify with protocol version.
        ChannelBuffer buf = ChannelBuffers.dynamicBuffer();
        
        //uhh, WTF is this? 
        // docs say protocol = v2, but this is what the official clients send.
        buf.writeInt(538990130);
        channel.write(buf);
        
        
        //IDENTIFY doesnt work!
//        NSQCommand command = new NSQCommand();
//        command.setLine("IDENTIFY");
//        command.addBytes("{\"short_id\":\"dustin-box\", \"long_id\":\"dustin-long-id\"}".getBytes("utf8"));
//        conn.command(command);
//        
        
        //subscribe
        conn.command(NSQCommand.instance("SUB test dustin"));
        //ready
        conn.command(NSQCommand.instance("RDY 100"));
        
	}
	
}
