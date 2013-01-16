JavaNSQCLient
=============

A fast netty based java client for nsq

Example usage:

Consumer

```
NSQLookup lookup = new NSQLookup();
lookup.addAddr("localhost", 4161);
NSQConsumer consumer = new NSQConsumer(lookup, "speedtest", "dustin", new MessageCallback() {
            
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


Producer

```
NSQProducer producer = new NSQProducer(50).addAddress("localhost", 4150);       
producer.start();
for (int i=0; i < 50000; i++) {
    producer.produce("speedtest", ("this is a message" + i).getBytes("utf8"));
}
```