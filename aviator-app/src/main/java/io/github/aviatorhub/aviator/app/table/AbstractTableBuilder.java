package io.github.aviatorhub.aviator.app.table;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import org.apache.commons.lang3.StringUtils;
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

  public abstract void build();

  protected StringBuilder tableBuild() {
    return new StringBuilder(schema)
        .append(" WITH ('connector' = '")
        .append(getConnIdentifier())
        .append("'");
  }
}
