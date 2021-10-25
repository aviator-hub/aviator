package io.github.aviatorhub.aviator.app.table;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

public abstract class TableOptBuilder {

  protected void appendOpt(StringBuilder builder, String opt, Object value) {
    if (value != null && StringUtils.isNotBlank(opt)) {
      builder.append(",").append("\n")
          .append(wrapValueQuota(opt))
          .append(" = ")
          .append(wrapValueQuota(value));
    }
  }

  private static String wrapValueQuota(Object value) {
    Preconditions.checkNotNull(value);
    if (value instanceof Integer
        || value instanceof Short
        || value instanceof Long
        || value instanceof Double
        || value instanceof Float) {
      return "'" + value + "'";
    } else if (value instanceof Enum) {
      return "'" + ((Enum<?>) value).name() + "'";
    } else {
      return "'" + value.toString() + "'";
    }
  }
}
