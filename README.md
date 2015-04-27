## JavaNSQClient

A (fast?) netty-based Java8 client for [NSQ](https://nsq.io)
heavily forked of TrendrrNSQClient.

## Artifact

```
<dependency>
  <groupId>com.github.brainlag</groupId>
  <artifactId>nsq-client</artifactId>
  <version>1.0.0.ALPHA</version>
</dependency>
```

## TODO:
* auth
* ssl
* backoff
* more tests
* better docu
* ....

## Consumer

Example usage:

```
NSQLookup lookup = new NSQLookup();
lookup.addAddr("localhost", 4161);
NSQConsumer consumer = new NSQConsumer(lookup, "speedtest", "dustin", (message) -> {
        System.out.println("received: " + message);            
        //now mark the message as finished.
        message.finished();
        
        //or you could requeue it, which indicates a failure and puts it back on the queue.
        //message.requeue();
});
        
consumer.start();
```

## Producer

Example usage: 

```
NSQProducer producer = new NSQProducer().addAddress("localhost", 4150).start();            
producer.produce("TestTopic", ("this is a message").getBytes());
```

