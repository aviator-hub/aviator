package io.github.aviatorhub.aviator.app.table;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import org.apache.flink.table.api.TableEnvironment;

public class KafkaTableBuilder extends AbstractTableBuilder<KafkaTableBuilder> {

  private String topic;
  private String group;
  private String format = "json";

  public KafkaTableBuilder(String schema, ConnectorConf conf,
      TableEnvironment tableEnv) {
    super(schema, conf, tableEnv);
    this.instance = this;
  }

  public KafkaTableBuilder topic(String topic) {
    this.topic = topic;
    return this;
  }

  @Override
  protected String getConnIdentifier() {
    return null;
  }

  @Override
  public void build() {

  }
}
