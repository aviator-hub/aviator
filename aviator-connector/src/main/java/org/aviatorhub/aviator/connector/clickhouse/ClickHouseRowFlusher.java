package org.aviatorhub.aviator.connector.clickhouse;

import static java.lang.String.format;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampType;
import org.aviatorhub.aviator.connector.ColumnInfo;
import org.aviatorhub.aviator.connector.ConnectorConf;
import org.aviatorhub.aviator.connector.exception.FlusherValidateException;
import org.aviatorhub.aviator.core.AbstractAviatorFlusher;
import org.elasticsearch.common.util.set.Sets;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.domain.ClickHouseFormat;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

@Slf4j
public class ClickHouseRowFlusher extends AbstractAviatorFlusher<RowData> {

  private static final Set<String> numberTypes = Sets
      .newHashSet("UInt8", "UInt16", "UInt32", "UInt64", "Int8", "Int16", "Int32", "Int64");

  private static final Set<String> floatTypes = Sets.newHashSet("Float32", "Float64");

  private static final Set<String> dateTypes = Sets.newHashSet("DateTime", "Date");

  private static final Map<LogicalTypeRoot, Set<String>> typeMatchMap = new HashMap<>();

  static {
    typeMatchMap.put(LogicalTypeRoot.CHAR, Sets.newHashSet("String", "Nullable(String)"));
    typeMatchMap.put(LogicalTypeRoot.BOOLEAN, Sets.newHashSet("UInt8"));
    typeMatchMap.put(LogicalTypeRoot.TINYINT, numberTypes);
    typeMatchMap.put(LogicalTypeRoot.SMALLINT, numberTypes);
    typeMatchMap.put(LogicalTypeRoot.INTEGER, numberTypes);
    typeMatchMap.put(LogicalTypeRoot.BIGINT, numberTypes);
    typeMatchMap.put(LogicalTypeRoot.FLOAT, floatTypes);
    typeMatchMap.put(LogicalTypeRoot.DOUBLE, floatTypes);
    typeMatchMap.put(LogicalTypeRoot.DATE, dateTypes);
    typeMatchMap.put(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE, dateTypes);
    typeMatchMap.put(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, dateTypes);
    typeMatchMap.put(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, dateTypes);
    typeMatchMap.put(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE, dateTypes);
  }


  // multiple clickhouse clusters is possible, so should not singleton
  private BalancedClickhouseDataSource dataSource;
  private final ConnectorConf conf;
  private final String insertSql;
  private List<String> colNameList;
  private Map<String, ColumnInfo> columnMap;

  public ClickHouseRowFlusher(ConnectorConf conf,
      List<Column> columns) throws Exception {
    super(conf.getSinkRetryCnt(), columns);
    this.conf = conf;
    this.colNameList = columns.stream().map(Column::getName).collect(Collectors.toList());
    insertSql = "INSERT INTO " + getFullTableName() + "("
        + this.colNameList.stream().collect(Collectors.joining(","))
        + ")";
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

    columnMap = columns.stream()
        .collect(Collectors.toMap(Column::getName, column -> {
          ColumnInfo columnInfo = new ColumnInfo();
          columnInfo.setNullable(false);
          columnInfo.setColumnName(column.getName());
          columnInfo.setColumnType(column.getDataType().getLogicalType());
          return columnInfo;
        }));

    String columnSql = "SELECT * FROM system.columns WHERE database = '"
        + StringUtils.defaultString(conf.getDatabase(), "default")
        + "' and table = '" + conf.getTable() + "'";
    try (ClickHouseConnection conn = getConnection();
        ClickHouseStatement stmt = conn.createStatement();
        ResultSet colResult = stmt.executeQuery(format(columnSql,
            conf.getDatabase(), conf.getTable()))) {
      while (colResult.next()) {
        String name = colResult.getString("name");
        String type = colResult.getString("type");
        ColumnInfo info = columnMap.get(name);
        if (info != null) {
          info.setStoreColumnType(type);
          info.setStoreColumnName(name);
        }
      }
    }

    List<String> nameNotMatchedMsgList = new LinkedList<>();
    List<String> typeNotMatchedMsgList = new LinkedList<>();
    for (Column column : columns) {
      ColumnInfo columnInfo = columnMap.get(column.getName());
      if (StringUtils.isBlank(columnInfo.getStoreColumnName())
          || StringUtils.isBlank(columnInfo.getStoreColumnType())) {
        nameNotMatchedMsgList.add(
            format("no column matches column name {}",
                columnInfo.getColumnName(), getFullTableName())
        );
      } else {
        LogicalTypeRoot type = columnInfo.getColumnType().getTypeRoot();
        Set<String> clickhouseTypeSet = typeMatchMap.get(type);
        if (clickhouseTypeSet == null) {
          typeNotMatchedMsgList.add(notSupportType(columnInfo));
          break;
        }

        if (!clickhouseTypeSet.contains(columnInfo.getStoreColumnType())) {
          typeNotMatchedMsgList.add(notMatchType(columnInfo));
        }
      }

      if (!nameNotMatchedMsgList.isEmpty() ||
          !typeNotMatchedMsgList.isEmpty()) {
        String nameNotMatchedMsg = StringUtils
            .defaultString(
                StringUtils.join(nameNotMatchedMsgList, "\n") + "\n",
                "");
        String typeNotMatchedMsg = StringUtils
            .defaultString(StringUtils.join(typeNotMatchedMsgList, "\n"));
        throw new FlusherValidateException(nameNotMatchedMsg + typeNotMatchedMsg);
      }
    }
  }

  private String notSupportType(ColumnInfo columnInfo) {
    return format("not supported type {} for column {}",
        columnInfo.getColumnType().getTypeRoot().name(), columnInfo.getColumnName());
  }

  private String notMatchType(ColumnInfo columnInfo) {
    return format("not match type {} for column {}",
        columnInfo.getColumnType().getTypeRoot().name(), columnInfo.getColumnName());
  }

  @Override
  protected void flush(RowData[] values) throws Exception {
    try (ClickHouseConnection conn = getConnection();
        ClickHouseStatement stmt = conn.createStatement()) {
      stmt.write().table(getFullTableName()).send(insertSql, stream -> {
        for (RowData row : values) {
          for (int i = 0; i < colNameList.size(); i++) {
            ColumnInfo columnInfo = columnMap.get(colNameList.get(i));
            assert columnInfo != null;
            // 处理空值
            if (columnInfo.isNullable()) {
              if (row.isNullAt(i)) {
                stream.markNextNullable(true);
                continue;
              } else {
                stream.markNextNullable(false);
              }
            }

            if ("UInt8".equals(columnInfo.getStoreColumnType())) {
              if (columnInfo.getColumnType().equals(LogicalTypeRoot.BOOLEAN)) {
                stream.writeUInt8(row.getBoolean(i));
              } else if (row.isNullAt(i)) {
                stream.writeUInt8(0);
              } else {
                int value = Math.max(row.getInt(i), 0);
                stream.writeUInt8(value);
              }
            } else if ("UInt16".equals(columnInfo.getStoreColumnType())) {
              if (row.isNullAt(i)) {
                stream.writeUInt16(0);
              } else {
                int value = Math.max(row.getInt(i),0);
                stream.writeUInt16(value);
              }
            } else if ("UInt32".equals(columnInfo.getStoreColumnType())) {
              if (row.isNullAt(i)) {
                stream.writeUInt32(0);
              } else {
                long value = Math.max(row.getLong(i), 0);
                stream.writeUInt32(value);
              }
            } else if ("UInt64".equals(columnInfo.getStoreColumnType())) {
              if (row.isNullAt(i)) {
                stream.writeUInt64(0);
              } else {
                long value = Math.max(row.getLong(i), 0);
                stream.writeUInt64(value);
              }
            } else if ("Int8".equals(columnInfo.getStoreColumnType())) {
              if (row.isNullAt(i)) {
                stream.writeInt8(0);
              } else {
                stream.writeInt8(row.getInt(i));
              }
            } else if ("Int16".equals(columnInfo.getStoreColumnType())) {
              if (row.isNullAt(i)) {
                stream.writeInt16(0);
              } else {
                stream.writeInt16(row.getInt(i));
              }
            } else if ("Int32".equals(columnInfo.getStoreColumnType())) {
              if (row.isNullAt(i)) {
                stream.writeInt32(0);
              } else {
                stream.writeInt32(row.getInt(i));
              }
            } else if ("Int64".equals(columnInfo.getStoreColumnType())) {
              if (row.isNullAt(i)) {
                stream.writeInt64(0);
              } else {
                stream.writeInt64(row.getLong(i));
              }
            } else if ("Float32".equals(columnInfo.getStoreColumnType())) {
              if (row.isNullAt(i)) {
                stream.writeFloat32(0);
              } else {
                stream.writeFloat32(row.getFloat(i));
              }
            } else if ("Float64".equals(columnInfo.getStoreColumnType())) {
              if (row.isNullAt(i)) {
                stream.writeFloat64(0);
              } else {
                stream.writeFloat64(row.getDouble(i));
              }
            } else if (Sets.newHashSet("Date", "DateTime").contains(columnInfo.getStoreColumnType())) {
              if (row.isNullAt(i)) {
                stream.writeDate(new Date(0));
              } else {
                if (columnInfo.getColumnType().equals(LogicalTypeRoot.DATE)) {
                  int days = row.getInt(i);
                  LocalDate localDate = LocalDate.ofEpochDay(days);
                  Date date = Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
                  stream.writeDate(date);
                } else {
                  TimestampData timestamp = row.getTimestamp(i, ((TimestampType) columnInfo.getColumnType()).getPrecision());
                  stream.writeDateTime(new Date(timestamp.getMillisecond()));
                }
              }
            } else {
              throw new IllegalArgumentException("handle clickhouse type failed.");
            }
          }
        }
      }, ClickHouseFormat.RowBinary);
    }
  }

  private String getFullTableName() {
    return StringUtils.defaultString(conf.getDatabase(), "default")
        + "." + conf.getTable();
  }

}
