package io.github.aviatorhub.aviator.connector.exception;

/**
 * @author meijie
 * @since 0.0.1
 */
public class TypeNotMatchedException extends Exception{

  public TypeNotMatchedException() {
    super();
  }

  public TypeNotMatchedException(String message) {
    super(message);
  }

  public TypeNotMatchedException(String message, Throwable cause) {
    super(message, cause);
  }

  public TypeNotMatchedException(Throwable cause) {
    super(cause);
  }

  protected TypeNotMatchedException(String message, Throwable cause,
      boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
