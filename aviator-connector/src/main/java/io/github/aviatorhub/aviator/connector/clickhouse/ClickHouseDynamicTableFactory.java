package io.github.aviatorhub.aviator.connector.clickhouse;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import io.github.aviatorhub.aviator.connector.ConnectorConfOptions;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.elasticsearch.common.util.set.Sets;

public class ClickHouseDynamicTableFactory implements DynamicTableSinkFactory {

  private static final String IDENTIFIER = "aviator-clickhouse";

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    final FactoryUtil.TableFactoryHelper helper = FactoryUtil
        .createTableFactoryHelper(this, context);
    final ReadableConfig config = helper.getOptions();
    ConnectorConf conf = buildConf(config);

    ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
    return new ClickHouseDynamicTableSink(conf, schema);
  }

  private ConnectorConf buildConf(ReadableConfig config) {
    ConnectorConf conf = new ConnectorConf();
    conf.setAddress(config.get(ConnectorConfOptions.ADDRESS));
    conf.setDatabase(config.get(ConnectorConfOptions.DATABASE));
    conf.setTable(config.get(ConnectorConfOptions.TABLE));
    conf.setUser(config.get(ConnectorConfOptions.USER));
    conf.setPasswd(config.get(ConnectorConfOptions.PASSWD));
    conf.setParallel(config.get(ConnectorConfOptions.PARALLEL));
    conf.setSinkBatchSize(config.get(ConnectorConfOptions.SINK_BATCH_SIZE));
    conf.setSinkFlushInterval(config.get(ConnectorConfOptions.SINK_FLUSH_INTERVAL));
    return conf;
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
