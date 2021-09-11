package io.github.aviatorhub.aviator.connector;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;

public class KeyExtractor implements Function<RowData, String>, Serializable {

  private final FieldFormatter[] fieldFormatters;
  private final String keyDelimiter;

  private interface FieldFormatter extends Serializable {

    String format(RowData rowData);
  }

  private KeyExtractor(FieldFormatter[] fieldFormatters, String keyDelimiter) {
    this.fieldFormatters = fieldFormatters;
    this.keyDelimiter = keyDelimiter;
  }

  @Override
  public String apply(RowData rowData) {
    final StringBuilder builder = new StringBuilder();
    for (int i = 0; i < fieldFormatters.length; i++) {
      if (i > 0) {
        builder.append(keyDelimiter);
      }
      final String value = fieldFormatters[i].format(rowData);
      builder.append(value);
    }
    return builder.toString();
  }

  private static class ColumnWithIndex {

    public Column column;
    public int index;

    public ColumnWithIndex(Column column, int index) {
      this.column = column;
      this.index = index;
    }

    public LogicalType getType() {
      return column.getDataType().getLogicalType();
    }

    public int getIndex() {
      return index;
    }
  }

  public static KeyExtractor createKeyExtractor(
      ResolvedSchema schema, String keyDelimiter) {
    return schema.getPrimaryKey()
        .map(key -> {
          Map<String, ColumnWithIndex> namesToColumns = new HashMap<>();
          List<Column> columnList = schema.getColumns();
          for (int i = 0; i < columnList.size(); i++) {
            Column column = columnList.get(i);
            namesToColumns.put(column.getName(), new ColumnWithIndex(column, i));
          }

          FieldFormatter[] fieldFormatters =
              key.getColumns().stream()
                  .map(namesToColumns::get)
                  .map(column -> toFormatter(column.index, column.getType()))
                  .toArray(FieldFormatter[]::new);

          return new KeyExtractor(fieldFormatters, keyDelimiter);

        }).orElse(null);
  }

  private static FieldFormatter toFormatter(
      int index, LogicalType type) {
    switch (type.getTypeRoot()) {
      case DATE:
        return (row) -> LocalDate.ofEpochDay(row.getInt(index)).toString();
      case TIME_WITHOUT_TIME_ZONE:
        return (row) ->
            LocalTime.ofNanoOfDay((long) row.getInt(index) * 1_000_000L).toString();
      case INTERVAL_YEAR_MONTH:
        return (row) -> Period.ofDays(row.getInt(index)).toString();
      case INTERVAL_DAY_TIME:
        return (row) -> Duration.ofMillis(row.getLong(index)).toString();
      case DISTINCT_TYPE:
        return toFormatter(index, ((DistinctType) type).getSourceType());
      default:
        RowData.FieldGetter fieldGetter = RowData.createFieldGetter(type, index);
        return (row) -> fieldGetter.getFieldOrNull(row).toString();
    }
  }
}
