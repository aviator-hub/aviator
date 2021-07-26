package org.aviatorhub.aviator.core;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.table.catalog.Column;

public abstract class AbstractAviatorFlusher<V> implements AviatorFlusher<V> {

  private final int retryCnt;

  public AbstractAviatorFlusher(int retryCnt) {
    this.retryCnt = retryCnt;
  }

  public AbstractAviatorFlusher(int retryCnt, List<Column> columns) throws Exception {
    this.retryCnt = retryCnt;
    validate(columns);
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
      if (retry < retryCnt) {
        doFlush(values, retry + 1);
      } else {
        throw e;
      }
    }
  }

  protected abstract void flush(V[] values) throws Exception;

  protected abstract void validate(List<Column> columns) throws Exception;
}
