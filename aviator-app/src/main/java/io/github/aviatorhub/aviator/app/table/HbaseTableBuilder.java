package io.github.aviatorhub.aviator.app.table;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import org.apache.flink.table.api.TableEnvironment;

public class HbaseTableBuilder extends AbstractTableBuilder<HbaseTableBuilder> {

  private String zkQuorum;
  private String zkNodeParent = "/hbase";
  private String nullLiteral;

  public HbaseTableBuilder(String schema, ConnectorConf conf, TableEnvironment tableEnv) {
    super(schema, conf, tableEnv);
    this.instance = this;
  }

  public HbaseTableBuilder zkNodeParent(String zkNodeParent) {
    this.zkNodeParent = zkNodeParent;
    return this;
  }

  public HbaseTableBuilder nullLiteral(String nullLiteral) {
    this.nullLiteral = nullLiteral;
    return this;
  }



  @Override
  protected String getConnIdentifier() {
    return "hbase-2.2";
  }

  @Override
  public void build() {
    StringBuilder builder = tableBuild();
    appendOpt(builder, "table-name", getFullyTableName());
    appendOpt(builder, "zookeeper.quorum", conf.getAddress());
    appendOpt(builder, "zookeeper.znode.parent", zkNodeParent);
    appendOpt(builder, "null-string-literal", nullLiteral);
    // TODO sink.buffer-flush.max-size
    appendOpt(builder, "sink.buffer-flush.max-rows", conf.getSinkBatchSize());
    appendOpt(builder, "sink.buffer-flush.interval", conf.getSinkFlushInterval());
    appendOpt(builder, "sink.parallelism", conf.getParallel());
    // TODO make sure async is always ture is reasonable.
    appendOpt(builder, "lookup.async", true);
    appendOpt(builder, "lookup.cache.max-rows", conf.getCacheSize());
    appendOpt(builder, "lookup.cache.ttl", conf.getCacheTime());
    appendOpt(builder, "lookup.max-retries", conf.getSinkRetryCnt());
    // TODO add properties
    builder.append(")");
    tableEnv.executeSql(builder.toString());
  }
}
