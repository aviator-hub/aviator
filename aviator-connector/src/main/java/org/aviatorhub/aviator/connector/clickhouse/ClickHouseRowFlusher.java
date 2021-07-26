package org.aviatorhub.aviator.connector.clickhouse;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.data.RowData;
import org.aviatorhub.aviator.connector.ConnectorConf;
import org.aviatorhub.aviator.core.AbstractAviatorFlusher;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

public class ClickHouseRowFlusher extends AbstractAviatorFlusher<RowData> {

  // multiple clickhouse clusters is possible, so should not singleton
  private BalancedClickhouseDataSource dataSource;
  private final ConnectorConf conf;

  public ClickHouseRowFlusher(ConnectorConf conf,
      List<Column> columns) throws Exception {
    super(conf.getSinkRetryCnt(), columns);
    this.conf = conf;
  }

  public ClickHouseConnection getConnection() throws SQLException {
    if (dataSource == null) {
      synchronized (this) {
        if (dataSource == null) {
          ClickHouseProperties properties = new ClickHouseProperties();
          properties.setUser(conf.getUser());
          properties.setPassword(conf.getPasswd());
          properties.setAsync(false);
          dataSource = new BalancedClickhouseDataSource(conf.getAddress(), properties);
          dataSource.scheduleActualization(10, TimeUnit.MINUTES);
          dataSource.withConnectionsCleaning(10, TimeUnit.MINUTES);
        }
      }
    }
    return dataSource.getConnection();
  }

  @Override
  protected void validate(List<Column> columns) throws Exception {
    String columnSql = "SELECT * FROM system.columns WHERE database = '"
        + StringUtils.defaultString(conf.getDatabase(), "default")
        + "' and table = '" + conf.getTable() + "'";

    try (ClickHouseConnection conn = getConnection();
        ClickHouseStatement stmt = conn.createStatement();
        ResultSet colResult = stmt.executeQuery(String.format(columnSql,
            conf.getDatabase(), conf.getTable()))) {
      while (colResult.next()) {
        String name = colResult.getString("name");
        String type = colResult.getString("type");

      }
    }
  }

  @Override
  protected void flush(RowData[] values) throws Exception {

  }

}
