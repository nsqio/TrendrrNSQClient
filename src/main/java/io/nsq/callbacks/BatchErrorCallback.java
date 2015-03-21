package io.nsq.callbacks;

import java.util.List;

public interface BatchErrorCallback {

    public void batchError(Exception ex, String topic, List<byte[]> messages);
}
