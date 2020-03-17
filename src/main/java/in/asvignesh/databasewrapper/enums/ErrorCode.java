package in.asvignesh.databasewrapper.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ErrorCode {

  SQL2O_IS_NULL(1000,
      "Sql2o instance is not configured successfully, please check your database configuration :)"),
  FROM_NOT_NULL(1001, "from class cannot be null, please check :)");

  private Integer code;
  private String msg;

}
