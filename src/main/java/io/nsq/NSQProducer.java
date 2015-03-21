package io.nsq;

import io.nsq.exceptions.BadMessageException;
import io.nsq.exceptions.BadTopicException;
import io.nsq.exceptions.NoConnectionsException;
import io.nsq.frames.ErrorFrame;
import io.nsq.frames.NSQFrame;
import io.nsq.frames.ResponseFrame;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class NSQProducer extends AbstractNSQClient {
    private List<ConnectionAddress> addresses = new ArrayList<ConnectionAddress>();

    private ConcurrentHashMap<String, Batch> batches = new ConcurrentHashMap<String, Batch>();

	/**
	 * If no connections are available, will try this many times with 5 second pause between, before throwing a
	 * no connections available exception.
	 */
    private int connectionRetries = 5;

    public void configureBatch(String topic, Integer maxMessages, Long maxBytes, Integer maxSeconds) {
        Batch batch = new Batch(topic);
        if (maxBytes != null) {
			batch.setMaxBytes(maxBytes);
		}
		if (maxMessages != null) {
			batch.setMaxMessages(maxMessages);
		}
		if (maxSeconds != null) {
			batch.setMaxSeconds(maxSeconds);
		}
		Batch old = this.batches.put(topic, batch);
		if (old != null)
			this.sendBatch(old, old.getAndClear());
	}

	/**
	 * flushes all batches
	 */
	public void flushBatches() {
		for (String k : batches.keySet()) {
			Batch b = batches.remove(k);
			if (b == null)
				continue;
			this.sendBatch(b, b.getAndClear());
		}
	}

	/**
	 * sends the batch, sending result to callback
	 * @param batch
	 */
    //TODO: fix this batch madness
    protected void sendBatch(Batch batch, List<byte[]> messages) {
		if (messages.isEmpty())
			return;
		try {
			produceMulti(batch.getTopic(), messages);
			//success!
		} catch (Exception x) {
		}
	}

	public void produceBatch(String topic, byte[] message) {
		if (!this.batches.containsKey(topic)) {
			//add the default setup.
            Batch def = new Batch(topic);

			this.batches.putIfAbsent(topic, def);
		}
		Batch batch = this.batches.get(topic);
		batch.addMessage(message);
		//send the batch if it is ready.
		List<byte[]> messages = batch.getAndClearIfReady();
		if (messages != null) {
			this.sendBatch(batch, messages);
		}
	}


	protected synchronized Connection getConn() throws NoConnectionsException {
		NoConnectionsException ex = new NoConnectionsException("no connections", null);
		for (int i=0; i < this.connectionRetries; i++) {
			try {
				return this.connections.next();
			} catch (NoConnectionsException x) {
				ex = x;
				try {
					Thread.sleep(5*1000);
				} catch (InterruptedException e) {}
				//try to reconnect..
                LogManager.getLogger(this).warn("Attempting to reconnect");
				this.connect();
			}
		}
        LogManager.getLogger(this).warn("Could not get a new connection within " + (this.connectionRetries * 5) + " seconds. giving up..");
		throw ex;
	}

	/**
	 * produce multiple messages.
	 */
    public void produceMulti(String topic, List<byte[]> messages) throws TimeoutException, BadTopicException, BadMessageException, NoConnectionsException {
        if (messages == null || messages.isEmpty()) {
            return;
		}

        if (messages.size() == 1) {
            //encoding will be screwed up if we MPUB a
            this.produce(topic, messages.get(0));
            return;
		}

		Connection c = this.getConn();

		NSQCommand command = NSQCommand.instance("MPUB " + topic);
        command.setData(messages);


		NSQFrame frame = c.commandAndWait(command);
		if (frame instanceof ResponseFrame) {
			c.setLastHeartbeat(); //TODO: remove once producer server heartbeats in place.
			return;
		}
		if (frame instanceof ErrorFrame) {
			String err = ((ErrorFrame)frame).getErrorMessage();
			if (err.startsWith("E_BAD_TOPIC")) {
				throw new BadTopicException(err);
			}
			if (err.startsWith("E_BAD_MESSAGE")) {
				throw new BadMessageException(err);
			}
		}
		//disconnect
		c.close();
	}

	/**
	 * @throws NoConnectionsException
	 */
    public void produce(String topic, byte[] message) throws BadTopicException, BadMessageException, NoConnectionsException, TimeoutException {
        Connection c = getConn();

		NSQCommand command = NSQCommand.instance("PUB " + topic, message);
		NSQFrame frame = c.commandAndWait(command);
		if (frame instanceof ResponseFrame) {
			c.setLastHeartbeat(); //TODO: remove once server heartbeats in place.
			return;
		}
		if (frame instanceof ErrorFrame) {
			String err = ((ErrorFrame)frame).getErrorMessage();
			if (err.startsWith("E_BAD_TOPIC")) {
				throw new BadTopicException(err);
			}
			if (err.startsWith("E_BAD_MESSAGE")) {
				throw new BadMessageException(err);
			}
		}
		//disconnect
		c.close();
	}

	public synchronized NSQProducer addAddress(String host, int port, int poolsize) {
		ConnectionAddress addr = new ConnectionAddress();
		addr.setHost(host);
		addr.setPoolsize(poolsize);
		addr.setPort(port);
		this.addresses.add(addr);
		return this;
	}

	@Override
	public void close() {
		flushBatches();
		super.close();
	}

	@Override
	public synchronized List<ConnectionAddress> lookupAddresses() {
		return this.addresses;
	}
}
