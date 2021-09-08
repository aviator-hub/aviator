package io.github.aviatorhub.aviator.connector.exception;

public class FlusherValidateException extends Exception {

  public FlusherValidateException() {
    super();
  }

  public FlusherValidateException(String message) {
    super(message);
  }

  public FlusherValidateException(String message, Throwable cause) {
    super(message, cause);
  }

  public FlusherValidateException(Throwable cause) {
    super(cause);
  }

  protected FlusherValidateException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
