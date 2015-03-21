package io.nsq.callbacks;

@FunctionalInterface
public interface BatchSuccessCallback {

    public void batchSuccess(String topic, int num);
}
