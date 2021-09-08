package io.github.aviatorhub.aviator.connector;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import org.apache.commons.io.FileUtils;
import org.apache.flink.shaded.netty4.io.netty.util.internal.ResourcesUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.utility.DockerImageName;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

/**
 * @author meijie
 * @since 0.0.1
 */
public class ClickHouseRowFlusherTest {

  @Rule
  protected ClickHouseContainer clickhouse = new ClickHouseContainer(
      DockerImageName.parse("yandex/clickhouse-server").withTag("20.8.19.4"));
  private ClickHouseDataSource dataSource;

  @Test
  public void testValidate() throws SQLException {

  }

  private void createTable(String sql) throws SQLException {
    try (ClickHouseConnection conn = dataSource.getConnection();
        ClickHouseStatement stmt = conn.createStatement()) {
      stmt.execute(sql);
    }
  }

  @Before
  public void before() throws IOException, SQLException {
    // create table
    ClickHouseProperties properties = new ClickHouseProperties();
    properties.setUser(clickhouse.getUsername());
    properties.setPassword(clickhouse.getPassword());
    dataSource = new ClickHouseDataSource(clickhouse.getJdbcUrl(), properties);

    File schemaValidateTableFile = ResourcesUtil.getFile(this.getClass(),
        "sql" + File.separator + "clickhouse_schema_validate_table.sql");
    String schemaValidateTableSql = FileUtils.readFileToString(schemaValidateTableFile, "utf-8");
    createTable(schemaValidateTableSql);
  }

}
