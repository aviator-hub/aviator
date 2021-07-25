package org.aviatorhub.aviator.connector.clickhouse;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.types.RowKind;

public class ClickHouseDynamicTableSink implements DynamicTableSink {

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
    return ChangelogMode.newBuilder()
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.UPDATE_AFTER)
        .build();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    return null;
  }

  @Override
  public DynamicTableSink copy() {
    return null;
  }

  @Override
  public String asSummaryString() {
    return null;
  }
}
