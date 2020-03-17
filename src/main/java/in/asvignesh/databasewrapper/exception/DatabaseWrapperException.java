package in.asvignesh.databasewrapper.exception;

import in.asvignesh.databasewrapper.enums.ErrorCode;
import lombok.Getter;

@Getter
public class DatabaseWrapperException extends RuntimeException {

  private static final long serialVersionUID = 3030374277105375809L;

  private Integer code;
  private String message;

  public DatabaseWrapperException() {
    super();
  }

  public DatabaseWrapperException(ErrorCode errorCode) {
    super(errorCode.getMsg());
    this.code = errorCode.getCode();
    this.message = errorCode.getMsg();
  }

  public DatabaseWrapperException(String message, Throwable cause) {
    super(message, cause);
  }

  public DatabaseWrapperException(String message) {
    super(message);
  }

  public DatabaseWrapperException(Throwable cause) {
    super(cause);
  }

}
