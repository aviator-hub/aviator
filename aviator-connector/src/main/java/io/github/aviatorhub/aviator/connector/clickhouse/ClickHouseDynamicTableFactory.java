package io.github.aviatorhub.aviator.connector.clickhouse;

import io.github.aviatorhub.aviator.connector.AbstractAviatorDynamicTableFactory;
import io.github.aviatorhub.aviator.connector.ConnectorConf;
import io.github.aviatorhub.aviator.connector.ConnectorConfOptions;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.elasticsearch.common.util.set.Sets;

public class ClickHouseDynamicTableFactory extends AbstractAviatorDynamicTableFactory implements
    DynamicTableSinkFactory {

  public static final String IDENTIFIER = "aviator-clickhouse";

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    ConnectorConf conf = buildConnectorConf(context);
    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
    return new ClickHouseDynamicTableSink(conf, schema);
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Sets.newHashSet(
        ConnectorConfOptions.ADDRESS,
        ConnectorConfOptions.TABLE,
        ConnectorConfOptions.USER,
        ConnectorConfOptions.PASSWD
    );
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Sets.newHashSet(
        ConnectorConfOptions.DATABASE,
        ConnectorConfOptions.PARALLEL,
        ConnectorConfOptions.SINK_BATCH_SIZE,
        ConnectorConfOptions.SINK_FLUSH_INTERVAL
    );
  }
}
