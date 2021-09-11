package io.github.aviatorhub.aviator.connector.redis;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import io.github.aviatorhub.aviator.connector.EnvMode;
import io.github.aviatorhub.aviator.connector.KeyExtractor;
import io.github.aviatorhub.aviator.core.AbstractAviatorFlusher;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.dynamic.Commands;
import io.lettuce.core.dynamic.RedisCommandFactory;
import io.lettuce.core.dynamic.batch.CommandBatching;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.flink.formats.json.RowDataToJsonConverters.RowDataToJsonConverter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.data.RowData;

public class RedisRowFlusher extends AbstractAviatorFlusher<RowData> {

  private final ConnectorConf conf;
  private final KeyExtractor keyExtractor;
  private final RowDataToJsonConverter valueConverter;

  private final RedisConnectionProvider client;
  private BatchingCommands batch;
  private final ObjectMapper MAPPER = new ObjectMapper();

  public RedisRowFlusher(
      ConnectorConf conf,
      KeyExtractor keyExtractor,
      RowDataToJsonConverter valueConverter) {
    super(conf.getSinkRetryCnt());
    this.conf = conf;
    this.keyExtractor = keyExtractor;
    this.valueConverter = valueConverter;

    this.client = new RedisConnectionProvider(conf);
    checkAndPrepareBatchingApi();
  }

  private synchronized void checkAndPrepareBatchingApi() {
    if (conf.getEnvMode().equals(EnvMode.CLUSTER)) {
      RedisCommandFactory factory = new RedisCommandFactory(client.getClusterConnection());
      batch = factory.getCommands(BatchingCommands.class);
    } else {
      RedisCommandFactory factory = new RedisCommandFactory(client.getConnection());
      batch = factory.getCommands(BatchingCommands.class);
    }
  }

  @Override
  protected void flush(RowData[] values) throws Exception {
    // TODO simple the code.
    if (conf.getSinkBufferCompaction()) {
      Map<String, String> keyValueMap = new HashMap<>();
      for (RowData value : values) {
        keyValueMap.put(keyExtractor.apply(value), toJsonValue(value));
      }
      int i = 1;
      for (Entry<String, String> entry : keyValueMap.entrySet()) {
        if (i == keyValueMap.size()) {
            if (conf.getDataExpireSecond() != null && conf.getDataExpireSecond() > 0) {
              batch.set(conf.getKeyPrefix() + entry.getKey(), entry.getValue(),
                  CommandBatching.queue());
              batch.expire(conf.getKeyPrefix() + entry.getKey(), conf.getDataExpireSecond(),
                  CommandBatching.flush());
            } else {
              batch.set(conf.getKeyPrefix() + entry.getKey(), entry.getValue(),
                  CommandBatching.flush());
            }
        } else {
          batch.set(entry.getKey(), entry.getValue(), CommandBatching.queue());
          if (conf.getDataExpireSecond() != null && conf.getDataExpireSecond() > 0) {
            batch.expire(entry.getKey(), conf.getDataExpireSecond(), CommandBatching.queue());
          }
        }
        i++;
      }
    } else {
      for (int i = 0; i < values.length; i++) {
        RowData value = values[i];
        if (i == values.length - 1) {

          if (conf.getDataExpireSecond() != null && conf.getDataExpireSecond() > 0) {
            batch.set(conf.getKeyPrefix() + keyExtractor.apply(value), toJsonValue(value),
                CommandBatching.queue());
            batch.expire(conf.getKeyPrefix() + keyExtractor.apply(value),
                conf.getDataExpireSecond(),
                CommandBatching.flush());
          } else {
            batch.set(conf.getKeyPrefix() + keyExtractor.apply(value), toJsonValue(value),
                CommandBatching.flush());
          }
        } else {
          batch.set(conf.getKeyPrefix() + keyExtractor.apply(value), toJsonValue(value),
              CommandBatching.queue());
          if (conf.getDataExpireSecond() != null && conf.getDataExpireSecond() > 0) {
            batch.expire(conf.getKeyPrefix() + keyExtractor.apply(value),
                conf.getDataExpireSecond(),
                CommandBatching.queue());
          }
        }
      }
    }
  }

  private String toJsonValue(RowData row) throws JsonProcessingException {
    JsonNode convert = valueConverter.convert(MAPPER, null, row);
    return MAPPER.writeValueAsString(convert);
  }

  @Override
  protected void validate(List<Column> columns) throws Exception {

  }

  interface BatchingCommands extends Commands {

    void set(String key, String value, CommandBatching commandBatching);

    void expire(String key, long seconds, CommandBatching commandBatching);
  }
}
