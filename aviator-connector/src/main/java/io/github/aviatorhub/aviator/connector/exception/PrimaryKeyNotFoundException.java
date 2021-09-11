package io.github.aviatorhub.aviator.connector.exception;

public class PrimaryKeyNotFoundException extends Exception {

  public PrimaryKeyNotFoundException() {
    super();
  }

  public PrimaryKeyNotFoundException(String message) {
    super(message);
  }

  public PrimaryKeyNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public PrimaryKeyNotFoundException(Throwable cause) {
    super(cause);
  }

  protected PrimaryKeyNotFoundException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
