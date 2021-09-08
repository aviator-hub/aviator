package io.github.aviatorhub.aviator.connector.exception;

/**
 * @author meijie
 * @since 0.0.1
 */
public class ColumnNameNotMatchedException extends Exception{

  public ColumnNameNotMatchedException() {
    super();
  }

  public ColumnNameNotMatchedException(String message) {
    super(message);
  }

  public ColumnNameNotMatchedException(String message, Throwable cause) {
    super(message, cause);
  }

  public ColumnNameNotMatchedException(Throwable cause) {
    super(cause);
  }

  protected ColumnNameNotMatchedException(String message, Throwable cause,
      boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
