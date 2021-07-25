package org.aviatorhub.aviator.connector.clickhouse;

import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import static org.aviatorhub.aviator.connector.ConnectorConfOptions.*;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.aviatorhub.aviator.connector.ConnectorConf;
import org.elasticsearch.common.util.set.Sets;

public class ClickHouseDynamicTableFactory implements DynamicTableSinkFactory {

  private static final String IDENTIFIER = "aviator-clickhouse";

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    final FactoryUtil.TableFactoryHelper helper = FactoryUtil
        .createTableFactoryHelper(this, context);
    final ReadableConfig config = helper.getOptions();
    ConnectorConf conf = buildConf(config);
    return new ClickHouseDynamicTableSink();
  }

  private ConnectorConf buildConf(ReadableConfig config) {
    ConnectorConf conf = new ConnectorConf();
    conf.setAddress(config.get(ADDRESS));
    conf.setDatabase(config.get(DATABASE));
    conf.setTable(config.get(TABLE));
    conf.setUser(config.get(USER));
    conf.setPassword(config.get(PASSWD));
    conf.setParallel(config.get(PARALLEL));
    conf.setSinkBatchSize(config.get(SINK_BATCH_SIZE));
    conf.setSinkFlushInterval(config.get(SINK_FLUSH_INTERVAL));
    return conf;
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Sets.newHashSet(
        ADDRESS,
        TABLE,
        USER,
        PASSWD
    );
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Sets.newHashSet(
        DATABASE,
        PARALLEL,
        SINK_BATCH_SIZE,
        SINK_FLUSH_INTERVAL
    );
  }
}
