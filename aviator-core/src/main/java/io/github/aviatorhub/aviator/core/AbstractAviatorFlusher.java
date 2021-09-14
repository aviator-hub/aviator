package io.github.aviatorhub.aviator.core;

import java.util.List;
import org.apache.flink.table.catalog.Column;
import org.jetbrains.annotations.NotNull;

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
  public void onFlush(V @NotNull [] values) throws Exception {
    doFlush(values, 0);
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
