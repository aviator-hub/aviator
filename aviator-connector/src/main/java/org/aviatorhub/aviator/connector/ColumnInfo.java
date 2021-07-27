package org.aviatorhub.aviator.connector;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.flink.table.types.logical.LogicalType;

@Getter
@NoArgsConstructor
public class ColumnInfo {
  private String columnName;
  private String storeColumnName;
  private LogicalType columnType;
  private String storeColumnType;
  private boolean isNullable = true;

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public void setStoreColumnName(String storeColumnName) {
    this.storeColumnName = storeColumnName;
  }

  public void setColumnType(LogicalType columnType) {
    this.columnType = columnType;
  }

  public void setStoreColumnType(String storeColumnType) {
    if ("Nullable(".startsWith(storeColumnType)) {
      this.isNullable = true;
      this.storeColumnType = storeColumnType.substring("Nullable(".length(), storeColumnType.length() -1);
    } else {
      this.storeColumnType = storeColumnType;
    }
  }

  public void setNullable(boolean nullable) {
    isNullable = nullable;
  }
}
