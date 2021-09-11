package io.github.aviatorhub.aviator.connector.exception;

public class UnsupportedConfigOptionException extends Exception {

  public UnsupportedConfigOptionException() {
    super();
  }

  public UnsupportedConfigOptionException(String message) {
    super(message);
  }

  public UnsupportedConfigOptionException(String message, Throwable cause) {
    super(message, cause);
  }

  public UnsupportedConfigOptionException(Throwable cause) {
    super(cause);
  }

  protected UnsupportedConfigOptionException(String message, Throwable cause,
      boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
