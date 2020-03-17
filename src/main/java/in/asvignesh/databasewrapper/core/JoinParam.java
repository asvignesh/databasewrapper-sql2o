package in.asvignesh.databasewrapper.core;

import in.asvignesh.databasewrapper.DataModel;
import in.asvignesh.databasewrapper.core.functions.TypeFunction;
import in.asvignesh.databasewrapper.enums.OrderBy;
import in.asvignesh.databasewrapper.utils.DatabaseUtils;
import lombok.Data;

@Data
public class JoinParam {

  private Class<? extends DataModel> joinModel;
  private String onLeft;
  private String onRight;
  private String fieldName;
  private String orderBy;

  public JoinParam(Class<? extends DataModel> joinModel) {
    this.joinModel = joinModel;
  }

  public <T, R> JoinParam as(TypeFunction<T, R> function) {
    String fieldName = DatabaseUtils.getLambdaColumnName(function);
    this.setFieldName(fieldName);
    return this;
  }

  public <T, S extends DataModel, R> JoinParam on(TypeFunction<T, R> left,
      TypeFunction<S, R> right) {
    String onLeft = DatabaseUtils.getLambdaFieldName(left);
    String onRight = DatabaseUtils.getLambdaColumnName(right);
    this.setOnLeft(onLeft);
    this.setOnRight(onRight);
    return this;
  }

  public <S extends DataModel, R> JoinParam order(TypeFunction<S, R> rightField,
      OrderBy orderBy) {
    String columnName = DatabaseUtils.getLambdaColumnName(rightField);
    this.orderBy = columnName + " " + orderBy.name();
    return this;
  }

  public JoinParam order(String orderBy) {
    this.orderBy = orderBy;
    return this;
  }
}
