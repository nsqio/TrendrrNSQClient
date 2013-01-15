/**
 * 
 */
package frames;

import java.io.UnsupportedEncodingException;

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
	
	public String getMessage() {
		try {
			return new String(this.data, "utf8");
		} catch (UnsupportedEncodingException e) {
			log.error("Caught", e);
		}
		return null;
	}
	
	public String toString() {
		return this.getMessage();
	}
}
