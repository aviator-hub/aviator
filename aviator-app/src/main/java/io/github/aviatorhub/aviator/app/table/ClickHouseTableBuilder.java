package io.github.aviatorhub.aviator.app.table;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import io.github.aviatorhub.aviator.connector.clickhouse.ClickHouseDynamicTableFactory;
import io.github.aviatorhub.aviator.connector.exception.UnsupportedConfigOptionException;
import org.apache.flink.table.api.TableEnvironment;

public class ClickHouseTableBuilder extends AbstractTableBuilder<ClickHouseTableBuilder> {

  public ClickHouseTableBuilder(String schema, ConnectorConf conf, TableEnvironment tableEnv) {
    super(schema, conf, tableEnv);
    this.instance = this;
  }

  @Override
  protected String getConnIdentifier() {
    return ClickHouseDynamicTableFactory.IDENTIFIER;
  }

  @Override
  public void build() throws UnsupportedConfigOptionException {
    ClickHouseDynamicTableFactory factory = new ClickHouseDynamicTableFactory();
    StringBuilder builder = tableBuild(factory.requiredOptions());
    appendOpts(builder, factory.optionalOptions());
    builder.append(")");
    tableEnv.executeSql(builder.toString());
  }

}
