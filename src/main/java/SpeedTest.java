/**
 * 
 */

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.trendrr.nsq.BatchCallback;
import com.trendrr.nsq.NSQMessageCallback;
import com.trendrr.nsq.NSQConsumer;
import com.trendrr.nsq.NSQLookup;
import com.trendrr.nsq.NSQMessage;
import com.trendrr.nsq.NSQProducer;
import com.trendrr.nsq.lookup.NSQLookupDynMapImpl;


/**
 * @author Dustin Norlander
 * @created Jan 24, 2013
 * 
 */
public class SpeedTest {

	protected static Logger log = LoggerFactory.getLogger(SpeedTest.class);
	static String address;
	static int port;
	
	/**
	 * @param args
	 * @throws UnsupportedEncodingException 
	 */
	public static void main(String[] args) throws UnsupportedEncodingException {
		
		address = "localhost";
		port = 4150;
		if (args.length > 0) {
			address = args[0];
			if (args.length > 1) {
				port = Integer.parseInt(args[1]);
			}
		}
		
		testProduceBatch();
//		testConsume();
	}
	
	public static void testConsume() {
		final int iterations = 100000;
		final Date start = new Date();
		final AtomicInteger processed = new AtomicInteger(0);
		
		NSQLookup lookup = new NSQLookupDynMapImpl();
        lookup.addAddr("localhost", 4161);
		

		/**
		 * Consumer
		 */
		NSQConsumer consumer = new NSQConsumer(lookup, "speedtest", "testconsumer", new NSQMessageCallback() {
			
			@Override
			public void message(NSQMessage message) {
				try {
					
					//now mark the message as finished.
					message.finished();
					
					//or you could requeue it, which indicates a failure and puts it back on the queue.
//					message.requeue();

					int p = processed.incrementAndGet();
					if (p % 1000 == 0) {
						System.out.println("consumer: " + p);
					}
					if (p == iterations) {
						System.out.println("completed " + iterations + " in " + (new Date().getTime()-start.getTime()));
					}
					
				} catch (Exception e) {
					log.error("Caught", e);
				}
			}
			
			@Override
			public void error(Exception x) {
				log.warn("Caught", x);
			}
		});
		
		System.out.println("Starting consumer");
		consumer.start();
		
		//TODO: have a wait latch 
		
	}
	
	public static void testProduceBatch() {
		int iterations = 100000;
		int batchsize = 100;
		int poolsize = 1;
		int messageSize = 10000; //number of bytes for a message.
		
		/*
		 * PRODUCER.  produce 50k messages
		 */
		//producer
		NSQProducer producer = new NSQProducer().addAddress(address, port, poolsize);		
		producer.configureBatch("speedtest", 
				new BatchCallback() {
					@Override
					public void batchSuccess(String topic, int num) {
					}
					@Override
					public void batchError(Exception ex, String topic, List<byte[]> messages) {
						ex.printStackTrace();	
					}
				}, 
			batchsize, 
			null, //use default maxbytes 
			null //use default max seconds
		);
		
		
		producer.start();
		Date start = new Date();
		
		StringBuilder str = new StringBuilder();
		for (int i=0; i < messageSize; i++) {
			str.append("a");
		}
		
		byte[] msg = str.toString().getBytes();
		

		for (int i=0; i < iterations; i++) {
			if (i % 1000 == 0) {
				System.out.println("produced: " + i);
			}
			producer.produceBatch("speedtest", msg);
		}
		
//		My System does this in about 10 seconds, so 5k messages per second on a single connection
		System.out.println("Produced " + iterations +" batch messages in " + (new Date().getTime()-start.getTime()) + " millis");
		
		producer.close();
	}
}
