/**
 * 
 */

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.trendrr.nsq.BatchCallback;
import com.trendrr.nsq.NSQProducer;
import com.trendrr.oss.StringHelper;


/**
 * @author Dustin Norlander
 * @created Jan 24, 2013
 * 
 */
public class SpeedTest {

	protected static Log log = LogFactory.getLog(SpeedTest.class);
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
		
		testBatch();
		
	}
	
	public static void testBatch() {
		int iterations = 100000;
		int batchsize = 100;
		int poolsize = 1;
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
		String msg = StringHelper.randomString(100);
		

		for (int i=0; i < iterations; i++) {
			if (i % 1000 == 0) {
				System.out.println("produced: " + i);
			}
			producer.produceBatch("speedtest", (msg + i).getBytes());
		}
		
//		My System does this in about 10 seconds, so 5k messages per second on a single connection
		System.out.println("Produced " + iterations +" batch messages in " + (new Date().getTime()-start.getTime()) + " millis");
		
		producer.close();
	}
}
