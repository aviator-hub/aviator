package io.github.aviatorhub.aviator.app.table;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import org.apache.flink.table.api.TableEnvironment;

public class RedisTableBuilder extends AbstractTableBuilder<RedisTableBuilder> {

  public RedisTableBuilder(String schema, ConnectorConf conf, TableEnvironment tableEnv) {
    super(schema, conf, tableEnv);
    this.instance = this;
  }

  @Override
  protected String getConnIdentifier() {
    return null;
  }

  @Override
  public void build() {

  }
}
