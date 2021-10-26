package io.github.aviatorhub.aviator.app.table;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import io.github.aviatorhub.aviator.connector.redis.RedisDynamicTableFactory;
import org.apache.flink.table.api.TableEnvironment;

public class RedisTableBuilder extends AbstractTableBuilder<RedisTableBuilder> {

  public RedisTableBuilder(String schema, ConnectorConf conf, TableEnvironment tableEnv) {
    super(schema, conf, tableEnv);
    this.instance = this;
  }

  @Override
  protected String getConnIdentifier() {
    return RedisDynamicTableFactory.IDENTIFIER;
  }

  @Override
  public void build() throws Exception {
    RedisDynamicTableFactory factory = new RedisDynamicTableFactory();
    StringBuilder builder = tableBuild(factory.requiredOptions());
    appendOpts(builder, factory.optionalOptions());
    builder.append(")");
    tableEnv.executeSql(builder.toString());
  }
}
