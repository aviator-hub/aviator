package io.github.aviatorhub.aviator.connector.clickhouse;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

public class ClickHouseDynamicTableSink implements DynamicTableSink {

  private final ConnectorConf conf;
  private final ResolvedSchema schema;

  public ClickHouseDynamicTableSink(ConnectorConf conf, ResolvedSchema schema) {
    this.conf = conf;
    this.schema = schema;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
    return ChangelogMode.newBuilder()
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.UPDATE_AFTER)
        .build();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    return SinkFunctionProvider.of(new ClickHouseRowSinkFunction(conf, schema));
  }

  @Override
  public DynamicTableSink copy() {
    return new ClickHouseDynamicTableSink(conf, schema);
  }

  @Override
  public String asSummaryString() {
    return "clickhouse dynamic sink table";
  }
}
