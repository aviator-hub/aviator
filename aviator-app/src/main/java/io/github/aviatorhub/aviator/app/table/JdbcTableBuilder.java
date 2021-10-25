package io.github.aviatorhub.aviator.app.table;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import org.apache.flink.table.api.TableEnvironment;

public class JdbcTableBuilder extends AbstractTableBuilder<JdbcTableBuilder> {

  private String driver;
  private String scanPartitionColumn;
  private Integer scanPartitionNumber;
  private Integer scanPartitionLowerBound;
  private Integer scanPartitionUpperBound;
  private Integer scanFetchSize;


  public JdbcTableBuilder(String schema, ConnectorConf conf, TableEnvironment tableEnv) {
    super(schema, conf, tableEnv);
    instance = this;
  }

  @Override
  protected String getConnIdentifier() {
    return "jdbc";
  }

  public JdbcTableBuilder driver(String driver) {
    this.driver = driver;
    return this;
  }

  public JdbcTableBuilder scanPartitionColumn(String scanPartitionColumn) {
    this.scanPartitionColumn = scanPartitionColumn;
    return this;
  }

  public JdbcTableBuilder scanPartitionNumber(Integer scanPartitionNumber) {
    this.scanPartitionNumber = scanPartitionNumber;
    return this;
  }

  public JdbcTableBuilder scanPartitionLowerBound(Integer scanPartitionLowerBound) {
    this.scanPartitionLowerBound = scanPartitionLowerBound;
    return this;
  }

  public JdbcTableBuilder scanPartitionUpperBound(Integer scanPartitionUpperBound) {
    this.scanPartitionUpperBound = scanPartitionUpperBound;
    return this;
  }

  public JdbcTableBuilder scanFetchSize(Integer scanFetchSize) {
    this.scanFetchSize = scanFetchSize;
    return this;
  }

  @Override
  public void build() {
    StringBuilder builder = tableBuild();
    appendOpt(builder, "url", conf.getAddress());
    appendOpt(builder, "table-name", getFullyTableName());
    appendOpt(builder, "driver", this.driver);
    appendOpt(builder, "username", conf.getUser());
    appendOpt(builder, "password", conf.getPasswd());
    // TODO appendOpt(builder, "connection.max-retry-timeout", );
    appendOpt(builder, "scan.partition.column", scanPartitionColumn);
    appendOpt(builder, "scan.partition.num", scanPartitionNumber);
    appendOpt(builder, "scan.partition.lower-bound", scanPartitionLowerBound);
    appendOpt(builder, "scan.partition.upper-bound", scanPartitionUpperBound);
    appendOpt(builder, "scan.fetch-size", scanFetchSize);
    // skip scan.auto-commit
    appendOpt(builder, "lookup.cache.max-rows", conf.getCacheSize());
    appendOpt(builder, "lookup.cache.ttl", conf.getCacheTime());
    appendOpt(builder, "lookup.max-retries", conf.getSinkRetryCnt());
    appendOpt(builder, "sink.buffer-flush.max-rows", conf.getSinkBatchSize());
    appendOpt(builder, "sink.buffer-flush.interval", conf.getSinkFlushInterval());
    appendOpt(builder, "sink.max-retries", conf.getSinkRetryCnt());
    appendOpt(builder, "sink.parallelism", conf.getParallel());
    builder.append(")");
    tableEnv.executeSql(builder.toString());
  }
}
