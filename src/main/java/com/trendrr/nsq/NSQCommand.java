package com.trendrr.nsq;
/**
 *
 */

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 *
 */
public class NSQCommand {

    protected static Logger log = LoggerFactory.getLogger(NSQCommand.class);

    public static NSQCommand instance(String line) {
        NSQCommand n = new NSQCommand();
        n.setLine(line);
        return n;
    }

    public static NSQCommand instance(String line, byte[] bytes) {
        NSQCommand n = instance(line);
        n.addBytes(bytes);
        return n;
    }

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

    public String toString() {
        return super.toString() + " (" + this.getLine().trim() + ")";
    }
}
