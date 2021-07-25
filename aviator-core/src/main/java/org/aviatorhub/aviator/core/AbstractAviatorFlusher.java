package org.aviatorhub.aviator.core;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractAviatorFlusher<V extends Serializable> implements AviatorFlusher<V> {

  private final AviatorBufferConf conf;

  public AbstractAviatorFlusher(AviatorBufferConf conf) {
    this.conf = conf;
  }

  @Override
  public void onFlush(V[] values) throws Exception {
    doFlush(values, 0);
  }

  @Override
  public void onFlush(V[] values, AtomicBoolean flushing) throws Exception {
    checkNotNull(flushing);
    try {
      doFlush(values, 0);
    } finally {
      flushing.set(false);
    }
  }

  private void doFlush(V[] values, int retry) throws Exception {
    try {
      flush(values);
    } catch (Exception e) {
      if (retry < conf.getRetryCnt()) {
        doFlush(values, retry + 1);
      } else {
        throw e;
      }
    }
  }

  protected abstract void flush(V[] values) throws Exception;

}
