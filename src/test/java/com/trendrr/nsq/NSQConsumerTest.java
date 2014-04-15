package com.trendrr.nsq;


import com.trendrr.nsq.lookup.NSQLookup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class NSQConsumerTest {

    private NSQConsumer nsqConsumer;

    private final String topic = "myTopic";

    private final String channel = "myChannel";

    private NSQLookup nsqLookup;

    private NSQMessageCallback callback;

    @Before
    public void setUp() throws Exception {
        nsqLookup = mock(NSQLookup.class);
        callback = mock(NSQMessageCallback.class);

        nsqConsumer = new NSQConsumer(nsqLookup, topic, channel, callback);
    }

    @Test
    public void testAddCommandsToNullConnectionReturnsNull() throws Exception {
        Assert.assertNull(nsqConsumer.addCommands(null));
    }

    @Test
    public void testAddCommandsToNoNullConnectionReturnsOriginalConnection() throws Exception {
        Connection mockConn = mock(Connection.class);
        Assert.assertEquals(mockConn, nsqConsumer.addCommands(mockConn));

        verify(mockConn, times(2)).command(any(NSQCommand.class));
    }
}
