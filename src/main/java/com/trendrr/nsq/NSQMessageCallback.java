/**
 *
 */
package com.trendrr.nsq;


/**
 * @author Dustin Norlander
 * @created Jan 15, 2013
 *
 */
public interface NSQMessageCallback {

    public void message(NSQMessage message);

    public void error(Exception x);
}
