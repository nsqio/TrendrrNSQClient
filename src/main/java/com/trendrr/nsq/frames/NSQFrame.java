/**
 *
 */
package com.trendrr.nsq.frames;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Dustin Norlander
 * @created Jan 14, 2013
 *
 */
public abstract class NSQFrame {

    protected static Logger log = LoggerFactory.getLogger(NSQFrame.class);
    int size;
    int frameId;
    byte[] data;


    public static NSQFrame instance(int type) {
        switch (type) {
        case 0 :
            return new ResponseFrame();
        case 1 :
            return new ErrorFrame();
        case 2 :
            return new MessageFrame();
        }
        return null;
    }


    public int getSize() {
        return size;
    }
    public void setSize(int size) {
        this.size = size;
    }
    public int getFrameId() {
        return frameId;
    }
    public void setFrameId(int frameId) {
        this.frameId = frameId;
    }
    public byte[] getData() {
        return data;
    }
    public void setData(byte[] data) {
        this.data = data;
    }


}
