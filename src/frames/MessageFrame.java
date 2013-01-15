/**
 * 
 */
package frames;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 * 
 */
public class MessageFrame extends NSQFrame {

	protected static Log log = LogFactory.getLog(MessageFrame.class);
	
	public MessageFrame() {
		this.frameId = 2;
	}
	
}
