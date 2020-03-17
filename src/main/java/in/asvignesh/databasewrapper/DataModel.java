package in.asvignesh.databasewrapper;

import in.asvignesh.databasewrapper.core.Query;
import in.asvignesh.databasewrapper.core.ResultKey;
import in.asvignesh.databasewrapper.core.functions.TypeFunction;
import java.io.Serializable;

public abstract class DataModel {

  /**
   * The query object for the current model.
   */
  private transient Query<? extends DataModel> query = new Query<>(
      this.getClass());

  /**
   * Save model
   *
   * @return ResultKey
   */
  public ResultKey save() {
    return query.save(this);
  }

  public ResultKey saveOrUpdateOnDuplicate() {
    return query.saveOrUpdateOnDuplicate(this);
  }

  /**
   * Update model
   *
   * @return number of rows affected after execution
   */
  public int update() {
    return query.updateByModel(this);
  }

  /**
   * Update by primary key
   *
   * @param id pk
   * @return number of rows affected after execution
   */
  public int updateById(Serializable id) {
    return new Query<>(this.getClass()).updateById(this, id);
  }

  /**
   * Delete model
   *
   * @return number of rows affected after execution
   */
  public int delete() {
    return query.deleteByModel(this);
  }

  /**
   * Update set statement
   *
   * @param column table column name [sql]
   * @param value column value
   * @return Query
   */
  public Query<? extends DataModel> set(String column, Object value) {
    return query.set(column, value);
  }

  /**
   * Update set statement with lambda
   *
   * @param function table column name with lambda
   * @param value column value
   * @return Query
   */
  public <T extends DataModel, R> Query<? extends DataModel> set(
      TypeFunction<T, R> function,
      Object value) {
    return query.set(function, value);
  }

  /**
   * Where statement
   *
   * @param statement conditional clause
   * @param value column value
   * @return Query
   */
  public Query<? extends DataModel> where(String statement, Object value) {
    return query.where(statement, value);
  }

  /**
   * Where statement with lambda
   *
   * @param function column name with lambda
   * @param value column value
   * @return Query
   */
  public <T extends DataModel, R> Query<? extends DataModel> where(
      TypeFunction<T, R> function,
      Object value) {
    return query.where(function, value);
  }

}
