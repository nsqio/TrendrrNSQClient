package commands;
/**
 * 
 */

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 * 
 */
public class NSQCommand {

	protected static Log log = LogFactory.getLog(NSQCommand.class);
	
	String line;
	List<byte[]> data = new ArrayList<byte[]>();
	
	public void addBytes(byte[] bytes) {
		data.add(bytes);
	}
	
	public String getLine() {
		return line;
	}
	public void setLine(String line) {
		if (!line.endsWith("\n")) {
			line = line +"\n";
		}
		
		this.line = line;
	}
	public List<byte[]> getData() {
		return data;
	}
	public void setData(List<byte[]> data) {
		this.data = data;
	}
	
}