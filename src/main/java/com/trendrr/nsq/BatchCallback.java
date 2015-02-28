package com.trendrr.nsq;

import java.util.List;

public interface BatchCallback {
	public void batchSuccess(String topic, int num);
	public void batchError(Exception ex, String topic, List<byte[]> messages);
}
