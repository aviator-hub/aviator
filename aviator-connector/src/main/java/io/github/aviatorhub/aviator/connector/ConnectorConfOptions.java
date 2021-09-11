package io.github.aviatorhub.aviator.connector;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class ConnectorConfOptions {

  // ==================================================================

  public static final ConfigOption<String> ADDRESS = ConfigOptions
      .key("address")
      .stringType()
      .noDefaultValue()
      .withDescription("the address to connect to the connector server");

  public static final ConfigOption<String> DATABASE = ConfigOptions
      .key("database")
      .stringType()
      .noDefaultValue()
      .withDescription("the database of the table to be written");

  public static final ConfigOption<String> TABLE = ConfigOptions
      .key("table")
      .stringType()
      .noDefaultValue()
      .withDescription("the table name to be written");

  public static final ConfigOption<String> USER = ConfigOptions
      .key("user")
      .stringType()
      .noDefaultValue()
      .withDescription("the user name to connect to the connector server");

  public static final ConfigOption<String> PASSWD = ConfigOptions
      .key("passwd")
      .stringType()
      .noDefaultValue()
      .withDescription("the password to connect to the clickhouse server");

  public static final ConfigOption<Integer> PARALLEL = ConfigOptions
      .key("parallel")
      .intType()
      .defaultValue(1)
      .withDescription("");

  public static final ConfigOption<Integer> SINK_BATCH_SIZE = ConfigOptions
      .key("sink.batch-size")
      .intType()
      .defaultValue(5000)
      .withDescription("the batch size to sink.");

  public static final ConfigOption<Integer> SINK_FLUSH_INTERVAL = ConfigOptions
      .key("sink.flush-interval")
      .intType()
      .defaultValue(5)
      .withDescription("the max interval between flushing");

  public static final ConfigOption<Boolean> SINK_ORDERED = ConfigOptions
      .key("sink.ordered")
      .booleanType()
      .noDefaultValue()
      .withDescription("flush the data with ordered.");

  public static final ConfigOption<String> KEY_PREFIX = ConfigOptions
      .key("key.prefix")
      .stringType()
      .defaultValue("")
      .withDescription("the prefix ot the key for kv store");

  public static final ConfigOption<Integer> SINK_DATA_EXPIRE = ConfigOptions
      .key("sink.data-expire")
      .intType()
      .noDefaultValue()
      .withDescription("the expire seconds for sinking data.");

  public static final ConfigOption<Boolean> SINK_BUFFER_COMPACTION = ConfigOptions
      .key("sink.buffer-compaction")
      .booleanType()
      .defaultValue(false)
      .withDescription("compact buffer data and only reserve latest data with same key before sinking");

}
