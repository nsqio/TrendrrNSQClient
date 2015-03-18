package com.trendrr.nsq;
/**
 *
 */


import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.trendrr.nsq.lookup.NSQLookupDynMapImpl;
import com.trendrr.oss.StringHelper;




/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 *
 */
public class ExampleMain {

    protected static Logger log = LoggerFactory.getLogger(ExampleMain.class);
    static AtomicInteger processed = new AtomicInteger(0);
    static Date start;
    /**
     * @param args
     */
    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
        /*
         * PRODUCER.  produce 50k messages
         */
        //producer
        NSQProducer producer = new NSQProducer().addAddress("localhost", 4150, 1);
        producer.start();
        start = new Date();
        String msg = StringHelper.randomString(10);

//        try {
//            producer.produce("test3", msg.getBytes());
//            System.out.println("PRODUCE 1");
//
//
//            List<byte[]> messages = new ArrayList<byte[]>();
//            messages.add(msg.getBytes());
//
//            producer.produceMulti("test3", messages);
//            if (true) {
//                System.exit(1);
//            }
//
//        } catch (DisconnectedException e1) {
//            log.error("Caught", e1);
//        } catch (BadTopicException e1) {
//            log.error("Caught", e1);
//        } catch (BadMessageException e1) {
//            log.error("Caught", e1);
//        }


//
//        for (int i=0; i < 50000; i++) {
//            if (i % 1000 == 0) {
//                System.out.println("producer: " + i);
//            }
//            try {
//                producer.produce("speedtest", (msg + i).getBytes("utf8"));
//            } catch (DisconnectedException e) {
//                log.error("Caught", e);
//            } catch (BadTopicException e) {
//                log.error("Caught", e);
//            } catch (BadMessageException e) {
//                log.error("Caught", e);
//            }
//        }
//
////        My System does this in about 10 seconds, so 5k messages per second on a single connection
//        System.out.println("Produced 50k messages in " + (new Date().getTime()-start.getTime()) + " millis");
//

        start = new Date();
        for (int i=0; i < 50000; i++) {
            if (i % 1000 == 0) {
                System.out.println("producer: " + i);
            }
            producer.produceBatch("speedtest2", (msg + i).getBytes("utf8"));
        }

//        My System does this in about 10 seconds, so 5k messages per second on a single connection
        System.out.println("Produced 50k batch messages in " + (new Date().getTime()-start.getTime()) + " millis");


        if (true)
            return;

        NSQLookup lookup = new NSQLookupDynMapImpl();
        lookup.addAddr("localhost", 4161);

        start = new Date();
        /**
         * Consumer.  consume 50k messages and immediately exit
         */
        NSQConsumer consumer = new NSQConsumer(lookup, "speedtest", "dustin", new NSQMessageCallback() {

            @Override
            public void message(NSQMessage message) {
                try {

                    //now mark the message as finished.
                    message.finished();

                    //or you could requeue it, which indicates a failure and puts it back on the queue.
//                    message.requeue();

                    int p = processed.incrementAndGet();
                    if (p % 1000 == 0) {
                        System.out.println("consumer: " + p);
                    }
                    if (p % 50000 == 0) {
                        System.out.println("completed 50k in " + (new Date().getTime()-start.getTime()));
                        start = new Date();
                        System.exit(1);
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

        consumer.start();



    }



}
