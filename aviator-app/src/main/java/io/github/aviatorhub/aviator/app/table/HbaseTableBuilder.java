package io.github.aviatorhub.aviator.app.table;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import org.apache.flink.table.api.TableEnvironment;

public class HbaseTableBuilder extends AbstractTableBuilder<HbaseTableBuilder> {

  private String zkQuorum;

  public HbaseTableBuilder(String schema, ConnectorConf conf, TableEnvironment tableEnv) {
    super(schema, conf, tableEnv);
    this.instance = this;
  }

  @Override
  protected String getConnIdentifier() {
    return "hbase-2";
  }

  @Override
  public void build() {
    StringBuilder builder = tableBuild();
    appendOpt(builder, "table-name", getFullyTableName());
  }
}
