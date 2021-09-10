package io.github.aviatorhub.aviator.connector.redis;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import io.github.aviatorhub.aviator.connector.KeyExtractor;
import io.github.aviatorhub.aviator.connector.exception.PrimaryKeyNotFoundException;
import lombok.SneakyThrows;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonOptions.MapNullKeyMode;
import org.apache.flink.formats.json.RowDataToJsonConverters;
import org.apache.flink.formats.json.RowDataToJsonConverters.RowDataToJsonConverter;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

public class RedisDynamicTableSink implements DynamicTableSink {

  private final ConnectorConf conf;
  private final ResolvedSchema schema;

  public RedisDynamicTableSink(ConnectorConf conf,
      ResolvedSchema schema) {
    this.conf = conf;
    this.schema = schema;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    return ChangelogMode.newBuilder()
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.UPDATE_AFTER)
        .build();
  }

  @SneakyThrows
  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    KeyExtractor keyExtractor = KeyExtractor.createKeyExtractor(schema, "_");
    if (keyExtractor == null) {
      throw new PrimaryKeyNotFoundException();
    }

    final RowDataToJsonConverter valueConverter =
        new RowDataToJsonConverters(TimestampFormat.SQL, MapNullKeyMode.DROP, "")
            .createConverter(schema.toSinkRowDataType().getLogicalType());
    return SinkFunctionProvider
        .of(new RedisRowSinkFunction(conf,  keyExtractor, valueConverter));
  }

  @Override
  public DynamicTableSink copy() {
    return new RedisDynamicTableSink(conf, schema);
  }

  @Override
  public String asSummaryString() {
    return "redis dynamic sink table";
  }
}
