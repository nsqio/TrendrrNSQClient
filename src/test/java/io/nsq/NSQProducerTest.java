package io.nsq;

import io.nsq.exceptions.NSQException;
import io.nsq.lookup.NSQLookup;
import org.apache.logging.log4j.LogManager;
import org.junit.Test;

import java.util.Date;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class NSQProducerTest {

    private NSQConfig getSnappyConfig() {
        final NSQConfig config = new NSQConfig();
        config.setCompression(NSQConfig.Compression.SNAPPY);
        return config;
    }

    private NSQConfig getDeflateConfig() {
        final NSQConfig config = new NSQConfig();
        config.setCompression(NSQConfig.Compression.DEFLATE);
        config.setDeflateLevel(4);
        return config;
    }

    @Test
    public void testProduceOneMsg() throws NSQException, TimeoutException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new NSQLookup();
        lookup.addAddr("localhost", 4161);

        NSQProducer producer = new NSQProducer();
        producer.setConfig(getSnappyConfig());
        producer.addAddress("localhost", 4150);
        producer.start();
        String msg = randomString();
        producer.produce("test3", msg.getBytes());
        producer.shutdown();

        NSQConsumer consumer = new NSQConsumer(lookup, "test3", "testconsumer", (message) -> {
            LogManager.getLogger(this).info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            message.finished();
        }, getSnappyConfig());
        consumer.start();
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.shutdown();
    }

    @Test
    public void testProduceOneMsgDeflate() throws NSQException, TimeoutException, InterruptedException {
        System.setProperty("io.netty.noJdkZlibDecoder", "false");
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new NSQLookup();
        lookup.addAddr("localhost", 4161);

        NSQProducer producer = new NSQProducer();
        producer.setConfig(getDeflateConfig());
        producer.addAddress("localhost", 4150);
        producer.start();
        String msg = randomString();
        producer.produce("test3", msg.getBytes());
        producer.shutdown();

        NSQConsumer consumer = new NSQConsumer(lookup, "test3", "testconsumer", (message) -> {
            LogManager.getLogger(this).info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            message.finished();
        }, getDeflateConfig());
        consumer.start();
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.shutdown();
    }

    @Test
    public void testProduceMoreMsg() throws NSQException, TimeoutException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new NSQLookup();
        lookup.addAddr("localhost", 4161);

        NSQConsumer consumer = new NSQConsumer(lookup, "test3", "testconsumer", (message) -> {
            LogManager.getLogger(this).info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            message.finished();
        });
        consumer.start();

        NSQProducer producer = new NSQProducer();
        producer.addAddress("localhost", 4150);
        producer.start();
        for (int i = 0; i < 1000; i++) {
            String msg = randomString();
            producer.produce("test3", msg.getBytes());
        }
        producer.shutdown();

        while (counter.get() < 1000) {
            Thread.sleep(500);
        }
        assertEquals(1000, counter.get());
        consumer.shutdown();
    }

    private String randomString() {
        return "Message" + new Date().getTime();
    }
}
