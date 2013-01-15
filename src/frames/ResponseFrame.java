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
public class ResponseFrame extends NSQFrame {

	protected static Log log = LogFactory.getLog(ResponseFrame.class);
	
	public ResponseFrame() {
		this.frameId = 0;
	}
}
