package io.github.aviatorhub.aviator.core;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.table.catalog.Column;

public class AviatorMockFlusher extends AbstractAviatorFlusher<Long> {

  public AtomicLong count = new AtomicLong(0);

  public AviatorMockFlusher(int retryCnt) {
    super(retryCnt);
  }

  @Override
  protected void flush(Long[] values) throws Exception {
    long expected = count.get();
    while (!count.compareAndSet(expected, expected + values.length)) {
      expected = count.get();
    }
  }

  public long getValue() {
    return count.get();
  }

  @Override
  protected void validate(List<Column> columns) throws Exception {

  }
}
