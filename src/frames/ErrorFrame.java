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
public class ErrorFrame extends NSQFrame {

	protected static Log log = LogFactory.getLog(ErrorFrame.class);
	
	public ErrorFrame() {
		this.frameId = 1;
	}
	
}
