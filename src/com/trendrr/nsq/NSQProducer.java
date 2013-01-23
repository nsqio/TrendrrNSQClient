/**
 * 
 */
package com.trendrr.nsq;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.trendrr.nsq.exceptions.BadMessageException;
import com.trendrr.nsq.exceptions.BadTopicException;
import com.trendrr.nsq.exceptions.DisconnectedException;
import com.trendrr.nsq.frames.ErrorFrame;
import com.trendrr.nsq.frames.NSQFrame;
import com.trendrr.nsq.frames.ResponseFrame;


/**
 * @author Dustin Norlander
 * @created Jan 22, 2013
 * 
 */
public class NSQProducer extends AbstractNSQClient {

	protected static Log log = LogFactory.getLog(NSQProducer.class);

	List<ConnectionAddress> addresses = new ArrayList<ConnectionAddress>();
	
	int batchSize = 100;
	
	ConcurrentHashMap<String, Batch> batches = new ConcurrentHashMap<String, Batch>();
	
	public void configureBatch(String topic, BatchCallback callback, int maxMessages, long maxBytes, int maxSeconds) {
		Batch batch = new Batch(topic, callback);
		batch.setMaxBytes(maxBytes);
		batch.setMaxMessages(maxMessages);
		batch.setMaxSeconds(maxSeconds);
		Batch old = this.batches.put(topic, batch);
		if (old != null) 
			this.sendBatch(old, old.getAndClear());
	}
	
	/**
	 * sends the batch, sending result to callback
	 * @param batch
	 */
	protected void sendBatch(Batch batch, List<byte[]> messages) {
		if (messages.isEmpty())
			return;
		try {
			this.produceMulti(batch.getTopic(), messages);
			//success!
			batch.getCallback().batchSuccess(batch.getTopic(), messages.size());
		} catch (Exception x) {
			batch.getCallback().batchError(x, batch.getTopic(), messages);
		}
	}
	
	/**
	 * produces a message in batch.  configure the batching options via
	 * configureBatch method.
	 * @param topic
	 * @param message
	 */
	public void produceBatch(String topic, byte[] message) {
		if (!this.batches.containsKey(topic)) {
			//add the default setup.
			Batch def = new Batch(topic, new BatchCallback() {
				@Override
				public void batchSuccess(String topic, int num) {
					//do nothing
				}
				@Override
				public void batchError(Exception ex, String topic, List<byte[]> messages) {
					log.error("Default batch callback for topic: " + topic, ex);
				}
			});
			
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
	
	/**
	 * produce multiple messages.
	 * @param topic
	 * @param message
	 * @throws DisconnectedException
	 * @throws BadTopicException
	 * @throws BadMessageException
	 */
	public void produceMulti(String topic, List<byte[]> message) throws DisconnectedException, BadTopicException, BadMessageException{
		Connection c = this.connections.next();
		
		NSQCommand command = NSQCommand.instance("MPUB " + topic + " " + message.size());
		command.setData(message);
		
		
		NSQFrame frame = c.commandAndWait(command);
		if (frame instanceof ResponseFrame) {
			c._setLastHeartbeat(); //remove once server heartbeats in place.
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
		throw new DisconnectedException("Error happened: " + frame, null);
		
	}
	
	/**
	 * @param topic
	 * @param message
	 */
	public void produce(String topic, byte[] message) throws DisconnectedException, BadTopicException, BadMessageException{
		Connection c = this.connections.next();
		
		NSQCommand command = NSQCommand.instance("PUB " + topic, message);
		NSQFrame frame = c.commandAndWait(command);
		if (frame instanceof ResponseFrame) {
			c._setLastHeartbeat(); //remove once server heartbeats in place.
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
		throw new DisconnectedException("Error happened: " + frame, null);
		
	}
	
	
	/**
	 * Adds a new connection.
	 * @param host
	 * @param port
	 * @param poolsize
	 */
	public synchronized NSQProducer addAddress(String host, int port, int poolsize) {
		ConnectionAddress addr = new ConnectionAddress();
		addr.setHost(host);
		addr.setPoolsize(poolsize);
		addr.setPort(port);
		this.addresses.add(addr);
		return this;
	}
	
	/* (non-Javadoc)
	 * @see com.trendrr.nsq.AbstractNSQClient#lookupAddresses()
	 */
	@Override
	public synchronized List<ConnectionAddress> lookupAddresses() {
		return this.addresses;
	}
}
