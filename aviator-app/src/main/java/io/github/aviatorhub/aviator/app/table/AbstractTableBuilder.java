package io.github.aviatorhub.aviator.app.table;

import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.ADDRESS;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.DATABASE;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.KEY_PREFIX;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.PARALLEL;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.PASSWD;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.SINK_BATCH_SIZE;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.SINK_BUFFER_COMPACTION;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.SINK_DATA_EXPIRE;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.SINK_FLUSH_INTERVAL;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.SINK_ORDERED;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.TABLE;
import static io.github.aviatorhub.aviator.connector.ConnectorConfOptions.USER;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import io.github.aviatorhub.aviator.connector.exception.UnsupportedConfigOptionException;
import java.util.Collection;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Convenient user api for user to create flink table.
 *
 * @param <T>
 * @author jie mei
 * @since 2021/10/22
 */
public abstract class AbstractTableBuilder<T> extends TableOptBuilder {

  protected final ConnectorConf conf;
  protected final TableEnvironment tableEnv;
  protected final String schema;

  protected T instance;

  public AbstractTableBuilder(String schema,
      ConnectorConf conf,
      TableEnvironment tableEnv) {
    this.schema = schema;
    this.conf = conf;
    this.tableEnv = tableEnv;
  }

  public T address(String address) {
    conf.setAddress(address);
    return instance;
  }

  public T database(String database) {
    conf.setDatabase(database);
    return instance;
  }

  public T table(String table) {
    conf.setTable(table);
    return instance;
  }

  public T user(String user) {
    conf.setUser(user);
    return instance;
  }

  public T passwd(String passwd) {
    conf.setPasswd(passwd);
    return instance;
  }

  public T parallel(Integer parallel) {
    conf.setParallel(parallel);
    return instance;
  }

  public T coreConnSize(Integer coreConnSize) {
    conf.setCoreConnSize(coreConnSize);
    return instance;
  }

  public T maxConnSize(Integer maxConnSize) {
    conf.setMaxConnSize(maxConnSize);
    return instance;
  }

  public T cacheSize(Integer cacheSize) {
    conf.setCacheSize(cacheSize);
    return instance;
  }

  public T cacheSecond(Integer second) {
    conf.setCacheTime(second);
    return instance;
  }

  public T sinkOrdered(Boolean ordered) {
    conf.setOrdered(ordered);
    return instance;
  }

  public T sinkRetryCnt(Integer sinkRetryCnt) {
    conf.setSinkRetryCnt(sinkRetryCnt);
    return instance;
  }

  public T sinkBatchSize(Integer sinkBatchSize) {
    conf.setSinkBatchSize(sinkBatchSize);
    return instance;
  }

  public T sinkFlushIntervalSecond(Integer sinkFlushInterval) {
    conf.setSinkFlushInterval(sinkFlushInterval);
    return instance;
  }

  public T sinkBufferCompaction(Boolean sinkBufferCompaction) {
    conf.setSinkBufferCompaction(sinkBufferCompaction);
    return instance;
  }

  protected T addProp(String key, String value) {
    conf.setProp(key, value);
    return instance;
  }

  protected String getFullyTableName() {
    if (StringUtils.isBlank(conf.getDatabase())) {
      return conf.getTable();
    } else {
      return conf.getDatabase() + "." + conf.getTable();
    }
  }

  protected abstract String getConnIdentifier();

  public abstract void build() throws Exception;

  protected StringBuilder tableBuild() {
    return new StringBuilder(schema)
        .append(" WITH ('connector' = '")
        .append(getConnIdentifier())
        .append("'");
  }

  protected StringBuilder tableBuild(Collection<ConfigOption<?>> optList)
      throws UnsupportedConfigOptionException {
    StringBuilder builder = tableBuild();
    for (ConfigOption<?> opt : optList) {
      appendOpt(builder, opt);
    }
    return builder;
  }

  protected StringBuilder appendOpts(StringBuilder builder, Collection<ConfigOption<?>> optionList)
      throws UnsupportedConfigOptionException {
    for (ConfigOption<?> opt : optionList) {
      appendOpt(builder, opt);
    }
    return builder;
  }

  private void appendOpt(StringBuilder builder, ConfigOption<?> opt)
      throws UnsupportedConfigOptionException {
    if (ADDRESS.equals(opt)) {
      appendOpt(builder, ADDRESS, conf.getAddress());
    } else if (DATABASE.equals(opt)) {
      appendOpt(builder, DATABASE, conf.getDatabase());
    } else if (TABLE.equals(opt)) {
      appendOpt(builder, TABLE, conf.getTable());
    } else if (USER.equals(opt)) {
      appendOpt(builder, USER, conf.getUser());
    } else if (PASSWD.equals(opt)) {
      appendOpt(builder, PASSWD, conf.getPasswd());
    } else if (PARALLEL.equals(opt)) {
      appendOpt(builder, PARALLEL, conf.getParallel());
    } else if (SINK_BATCH_SIZE.equals(opt)) {
      appendOpt(builder, SINK_BATCH_SIZE, conf.getSinkBatchSize());
    } else if (SINK_FLUSH_INTERVAL.equals(opt)) {
      appendOpt(builder, SINK_FLUSH_INTERVAL, conf.getSinkFlushInterval());
    } else if (SINK_ORDERED.equals(opt)) {
      appendOpt(builder, SINK_ORDERED, conf.getOrdered());
    } else if (KEY_PREFIX.equals(opt)) {
      appendOpt(builder, KEY_PREFIX, conf.getKeyPrefix());
    } else if (SINK_DATA_EXPIRE.equals(opt)) {
      appendOpt(builder, SINK_DATA_EXPIRE, conf.getKeyPrefix());
    } else if (SINK_BUFFER_COMPACTION.equals(opt)) {
      appendOpt(builder, SINK_BUFFER_COMPACTION, conf.getSinkBufferCompaction());
    } else {
      throw new UnsupportedConfigOptionException();
    }
  }

}
