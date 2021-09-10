package io.github.aviatorhub.aviator.connector.exception;

/**
 * @author meijie
 * @since 0.0.1
 */
public class NameNotMatchedException extends Exception{

  public NameNotMatchedException() {
    super();
  }

  public NameNotMatchedException(String message) {
    super(message);
  }

  public NameNotMatchedException(String message, Throwable cause) {
    super(message, cause);
  }

  public NameNotMatchedException(Throwable cause) {
    super(cause);
  }

  protected NameNotMatchedException(String message, Throwable cause,
      boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
