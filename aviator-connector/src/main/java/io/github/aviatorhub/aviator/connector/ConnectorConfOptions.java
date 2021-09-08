package io.github.aviatorhub.aviator.connector;

import org.apache.flink.configuration.ConfigOption;

public class ConnectorConfOptions {

  public static final ConfigOption<String> ADDRESS = org.apache.flink.configuration.ConfigOptions
      .key("address")
      .stringType()
      .noDefaultValue()
      .withDescription("the address to connect to the connector server");

  public static final ConfigOption<String> DATABASE = org.apache.flink.configuration.ConfigOptions
      .key("database")
      .stringType()
      .noDefaultValue()
      .withDescription("the database of the table to be written");

  public static final ConfigOption<String> TABLE = org.apache.flink.configuration.ConfigOptions
      .key("table")
      .stringType()
      .noDefaultValue()
      .withDescription("the table name to be written");

  public static final ConfigOption<String> USER = org.apache.flink.configuration.ConfigOptions
      .key("user")
      .stringType()
      .noDefaultValue()
      .withDescription("the user name to connect to the connector server");

  public static final ConfigOption<String> PASSWD = org.apache.flink.configuration.ConfigOptions
      .key("passwd")
      .stringType()
      .noDefaultValue()
      .withDescription("the password to connect to the clickhouse server");

  public static final ConfigOption<Integer> PARALLEL = org.apache.flink.configuration.ConfigOptions
      .key("parallel")
      .intType()
      .defaultValue(1)
      .withDescription("");

  public static final ConfigOption<Integer> SINK_BATCH_SIZE = org.apache.flink.configuration.ConfigOptions
      .key("sink.batch-size")
      .intType()
      .defaultValue(5000)
      .withDescription("");

  public static final ConfigOption<Integer> SINK_FLUSH_INTERVAL = org.apache.flink.configuration.ConfigOptions
      .key("sink.flush-interval")
      .intType()
      .defaultValue(5)
      .withDescription("");

}
