/**
 *
 */
package com.trendrr.nsq.exceptions;



/**
 * @author Dustin Norlander
 * @created Jan 22, 2013
 *
 */
public class BadTopicException extends Exception {

    private static final long serialVersionUID = 5128064038299401754L;

    public BadTopicException(String message) {
        super(message);
    }
}
