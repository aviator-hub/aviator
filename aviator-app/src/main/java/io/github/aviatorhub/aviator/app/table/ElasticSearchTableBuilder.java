package io.github.aviatorhub.aviator.app.table;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import org.apache.flink.table.api.TableEnvironment;

public class ElasticSearchTableBuilder extends AbstractTableBuilder<ElasticSearchTableBuilder> {

  private String docIdKeyDelimiter;

  public ElasticSearchTableBuilder(String schema,
      ConnectorConf conf,
      TableEnvironment tableEnv) {
    super(schema, conf, tableEnv);
    this.instance = this;
  }

  @Override
  protected String getConnIdentifier() {
    return "elasticsearch-7";
  }

  public ElasticSearchTableBuilder docIdKeyDelimiter(String docIdKeyDelimiter) {
    this.docIdKeyDelimiter = docIdKeyDelimiter;
    return this;
  }


  @Override
  public void build() {
    StringBuilder builder = tableBuild();
    appendOpt(builder, "hosts", conf.getAddress());
    appendOpt(builder, "index", conf.getTable());
    appendOpt(builder, "document-id.key-delimiter", docIdKeyDelimiter);
    appendOpt(builder, "username", conf.getUser());
    appendOpt(builder, "password", conf.getPasswd());
    // TODO failure-handler
    appendOpt(builder, "sink.bulk-flush.max-actions", conf.getSinkBatchSize());
    // TODO sink.bulk-flush.max-size
    appendOpt(builder, "sink.bulk-flush.interval", conf.getSinkFlushInterval());

//    sink.bulk-flush.backoff.strategy
//    sink.bulk-flush.backoff.max-retries
//    sink.bulk-flush.backoff.delay
//    connection.max-retry-timeout
//    connection.path-prefix
//    format
    builder.append(")");
    tableEnv.executeSql(builder.toString());
  }
}
