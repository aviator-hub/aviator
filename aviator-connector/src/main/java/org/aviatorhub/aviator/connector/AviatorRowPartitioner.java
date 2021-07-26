package org.aviatorhub.aviator.connector;

import org.apache.flink.table.data.RowData;
import org.aviatorhub.aviator.core.AviatorPartitioner;

public class AviatorRowPartitioner implements AviatorPartitioner<RowData> {

  private ConnectorConf conf;

  @Override
  public int calcPartition(RowData rowData) {
    return 0;
  }
}
