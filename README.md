## TrendrrNSQClient

A fast netty-based java client for [NSQ][nsq] initially developed and maintained by [@dustismo](https://github.com/dustismo)
for use in various places in the trendrr.tv and curatorr.com stacks.

**I ([@mreiferson](https://github.com/mreiferson)) am currently maintaining this repo to facilitate
bug fixes and minor updates and am interested in finding a longer-tem maintainer.**

## Consumer

Example usage:

```
NSQLookup lookup = new NSQLookupDynMapImpl();
lookup.addAddr("localhost", 4161);
NSQConsumer consumer = new NSQConsumer(lookup, "speedtest", "dustin", new NSQMessageCallback() {
            
    @Override
    public void message(NSQMessage message) {
        System.out.println("received: " + message);            
        //now mark the message as finished.
        message.finished();
        
        //or you could requeue it, which indicates a failure and puts it back on the queue.
        //message.requeue();
    }           
    @Override
    public void error(Exception x) {
        //handle errors
        log.warn("Caught", x);
    }
});
        
consumer.start();
```


## Producer

Example usage: 

```
NSQProducer producer = new NSQProducer().addAddress("localhost", 4150, 1);            
producer.start();
for (int i=0; i < 50000; i++) {
    producer.produce("speedtest", ("this is a message" + i).getBytes());
}
```

The producer also has a Batch collector that will collect messages until some threshold is reached (currently maxbytes or maxmessages) then send as a MPUB request.  This gives much greater throughput then producing messages one at a time.

```
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
for (int i=0; i < iterations; i++) {
    producer.produceBatch("speedtest", ("this is a message" + i).getBytes());
}
```


## Dependancies

* [netty][netty]
* [slf4j][slf4j]
* [trendrr-oss][trendrr-oss]

Note: the trendrr-oss dependancy can easily be swapped out by implementing the com.trendrr.nsq.NSQLookup interface using a different json parser


[nsq]: https://github.com/bitly/nsq
[netty]: http://netty.io/
[slf4j]: http://www.slf4j.org/
[trendrr-oss]: https://github.com/trendrr/java-oss-lib
