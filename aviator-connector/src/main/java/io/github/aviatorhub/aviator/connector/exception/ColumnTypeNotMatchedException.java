package io.github.aviatorhub.aviator.connector.exception;

/**
 * @author meijie
 * @since 0.0.1
 */
public class ColumnTypeNotMatchedException extends Exception{

  public ColumnTypeNotMatchedException() {
    super();
  }

  public ColumnTypeNotMatchedException(String message) {
    super(message);
  }

  public ColumnTypeNotMatchedException(String message, Throwable cause) {
    super(message, cause);
  }

  public ColumnTypeNotMatchedException(Throwable cause) {
    super(cause);
  }

  protected ColumnTypeNotMatchedException(String message, Throwable cause,
      boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
