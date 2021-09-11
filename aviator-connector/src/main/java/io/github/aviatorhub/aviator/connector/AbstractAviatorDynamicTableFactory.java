package io.github.aviatorhub.aviator.connector;

import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.ADDRESS;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.DATABASE;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.KEY_PREFIX;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.PARALLEL;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.PASSWD;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.SINK_BATCH_SIZE;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.SINK_DATA_EXPIRE;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.SINK_FLUSH_INTERVAL;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.SINK_ORDERED;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.TABLE;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.USER;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

public abstract class AbstractAviatorDynamicTableFactory implements DynamicTableFactory {

  protected ConnectorConf buildConnectorConf(Context context) {
    final FactoryUtil.TableFactoryHelper helper = FactoryUtil
        .createTableFactoryHelper(this, context);
    final ReadableConfig readable = helper.getOptions();
    final ConnectorConf conf = new ConnectorConf();
    requiredOptions().forEach(option -> applyConfOption(conf, readable, option));
    optionalOptions().forEach(option -> applyConfOption(conf, readable, option));
    return conf;
  }

  private void applyConfOption(
      ConnectorConf conf,
      ReadableConfig readable,
      ConfigOption<?> option) {
    switch (option.key()) {
      case "address":
        conf.setAddress(readable.get(ADDRESS));
        break;
      case "database":
        conf.setDatabase(readable.get(DATABASE));
        break;
      case "table":
        conf.setTable(readable.get(TABLE));
        break;
      case "user":
        conf.setUser(readable.get(USER));
        break;
      case "passwd":
        conf.setPasswd(readable.get(PASSWD));
        break;
      case "parallel":
        conf.setParallel(readable.get(PARALLEL));
        break;
      case "sink.batch-size":
        conf.setSinkBatchSize(readable.get(SINK_BATCH_SIZE));
        break;
      case "sink.flush-interval":
        conf.setSinkFlushInterval(readable.get(SINK_FLUSH_INTERVAL));
        break;
      case "sink.ordered":
        conf.setOrdered(readable.get(SINK_ORDERED));
        break;
      case "key.prefix":
        conf.setKeyPrefix(readable.get(KEY_PREFIX));
        break;
      case "sink.data-expire":
        conf.setDataExpireSecond(readable.get(SINK_DATA_EXPIRE));
        break;
      case "sink.buffer-compaction":

      default:
        throw new RuntimeException(option.key()
            + " is not supported for "
            + factoryIdentifier() + " dynamic table.");
    }
  }

}
