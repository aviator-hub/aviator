package io.github.aviatorhub.aviator.connector;

import io.github.aviatorhub.aviator.connector.redis.RedisClient;
import io.github.aviatorhub.aviator.connector.redis.RedisRowFlusher;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonOptions.MapNullKeyMode;
import org.apache.flink.formats.json.RowDataToJsonConverters;
import org.apache.flink.formats.json.RowDataToJsonConverters.RowDataToJsonConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.GenericRowData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class RedisRowFlusherTest {

  private String host;
  private Integer port;

  @Rule
  public GenericContainer redisContainer = new GenericContainer(
      DockerImageName.parse("redis:6.2.5")).withExposedPorts(6379);

  @Before
  public void before() {
    host = redisContainer.getHost();
    port = redisContainer.getFirstMappedPort();
  }

  @Test
  public void testFlush() throws Exception {
    ConnectorConf conf = new ConnectorConf();
    conf.setKeyPrefix("test_");
    conf.setAddress(host + ":" + port);

    ResolvedSchema schema = buildSchema();
    KeyExtractor keyExtractor = KeyExtractor.createKeyExtractor(schema, "_");
    final RowDataToJsonConverter valueConverter =
        new RowDataToJsonConverters(TimestampFormat.SQL, MapNullKeyMode.DROP, "")
            .createConverter(schema.toSinkRowDataType().getLogicalType());
    RedisRowFlusher flusher = new RedisRowFlusher(conf, keyExtractor, valueConverter);

    GenericRowData[] rows = new GenericRowData[]{
        GenericRowData.of(1, 1, "test"),
        GenericRowData.of(2, 2, "test1"),
        GenericRowData.of(1, 1, "test2")
    };
    flusher.onFlush(rows);

    RedisClient redisClient = new RedisClient(conf);
    RedisAdvancedClusterCommands<String, String> sync = redisClient.getConnection().sync();
    Assert.assertEquals("test2", sync.get("test_1"));
  }

  private ResolvedSchema buildSchema() {
    return new ResolvedSchema(
        Arrays.asList(
            Column.physical("id", DataTypes.INT().notNull()),
            Column.physical("counter", DataTypes.INT().notNull()),
            Column.physical("topic", DataTypes.STRING().notNull())),
        new LinkedList<>(),
        UniqueConstraint.primaryKey(
            "primary_constraint", Collections.singletonList("id")));

  }

}
