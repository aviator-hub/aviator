package io.github.aviatorhub.aviator.connector.redis;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import io.github.aviatorhub.aviator.core.AbstractAviatorFlusher;
import java.util.List;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.data.RowData;

public class RedisRowFlusher extends AbstractAviatorFlusher<RowData> {

  private final ConnectorConf conf;

  public RedisRowFlusher(ConnectorConf conf) {
    super(conf.getSinkRetryCnt());
    this.conf = conf;
  }

  @Override
  protected void flush(RowData[] values) throws Exception {

  }

  @Override
  protected void validate(List<Column> columns) throws Exception {

  }
}
