package io.github.aviatorhub.aviator.connector.redis;

import com.google.common.collect.Sets;
import io.github.aviatorhub.aviator.connector.AbstractAviatorDynamicTableFactory;
import io.github.aviatorhub.aviator.connector.ConnectorConf;
import io.github.aviatorhub.aviator.connector.ConnectorConfOptions;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;

public class RedisDynamicTableFactory extends AbstractAviatorDynamicTableFactory implements DynamicTableSinkFactory {

  public static String IDENTIFIER = "aviator-redis";

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    ConnectorConf conf = buildConnectorConf(context);
    ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
    return new RedisDynamicTableSink(conf, resolvedSchema);
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Sets.newHashSet(
        ConnectorConfOptions.ADDRESS
    );
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Sets.newHashSet(
        ConnectorConfOptions.KEY_PREFIX,
        ConnectorConfOptions.USER,
        ConnectorConfOptions.PASSWD,

        ConnectorConfOptions.PARALLEL,
        ConnectorConfOptions.SINK_BATCH_SIZE,
        ConnectorConfOptions.SINK_FLUSH_INTERVAL,
        ConnectorConfOptions.SINK_ORDERED,
        ConnectorConfOptions.SINK_DATA_EXPIRE
    );
  }
}
