package com.github.brainlag.nsq;

import com.github.brainlag.nsq.exceptions.NSQException;
import com.github.brainlag.nsq.lookup.NSQLookup;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.junit.Test;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    public void testProduceOneMsgSnappy() throws NSQException, TimeoutException, InterruptedException {
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
        assertTrue(counter.get() >= 1000);
        consumer.shutdown();
    }

    @Test
    public void testParallelProducer() throws NSQException, TimeoutException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new NSQLookup();
        lookup.addAddr("localhost", 4161);

        NSQConsumer consumer = new NSQConsumer(lookup, "test3", "testconsumer", (message) -> {
            LogManager.getLogger(this).info("Processing message: " + new String(message.getMessage()));
            counter.incrementAndGet();
            message.finished();
        });
        consumer.start();

        for (int n = 0; n < 5; n++) {
            new Thread(() -> {
                NSQProducer producer = new NSQProducer();
                producer.addAddress("localhost", 4150);
                producer.start();
                for (int i = 0; i < 1000; i++) {
                    String msg = randomString();
                    try {
                        producer.produce("test3", msg.getBytes());
                    } catch (NSQException | TimeoutException e) {
                        Throwables.propagate(e);
                    }
                }
                producer.shutdown();
            }).start();
        }
        while (counter.get() < 5000) {
            Thread.sleep(500);
        }
        assertTrue(counter.get() >= 5000);
        consumer.shutdown();
    }

    @Test
    public void testMulitMessage() throws NSQException, TimeoutException, InterruptedException {
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
        List<byte[]> messages = Lists.newArrayList();
        for (int i = 0; i < 50; i++) {
            messages.add(randomString().getBytes());
        }
        producer.produceMulti("test3", messages);
        producer.shutdown();

        while (counter.get() < 50) {
            Thread.sleep(500);
        }
        assertTrue(counter.get() >= 50);
        consumer.shutdown();
    }

    private String randomString() {
        return "Message" + new Date().getTime();
    }
}
