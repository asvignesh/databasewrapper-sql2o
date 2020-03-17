package in.asvignesh.databasewrapper;

import static in.asvignesh.databasewrapper.enums.ErrorCode.SQL2O_IS_NULL;
import static in.asvignesh.databasewrapper.utils.Functions.ifReturn;
import static in.asvignesh.databasewrapper.utils.Functions.ifReturnOrThrow;
import static in.asvignesh.databasewrapper.utils.Functions.ifThrow;
import static java.util.stream.Collectors.joining;

import in.asvignesh.databasewrapper.core.Atomic;
import in.asvignesh.databasewrapper.core.Query;
import in.asvignesh.databasewrapper.core.ResultKey;
import in.asvignesh.databasewrapper.core.dml.Delete;
import in.asvignesh.databasewrapper.core.dml.Select;
import in.asvignesh.databasewrapper.core.dml.Update;
import in.asvignesh.databasewrapper.core.functions.TypeFunction;
import in.asvignesh.databasewrapper.dialect.Dialect;
import in.asvignesh.databasewrapper.dialect.MySQLDialect;
import in.asvignesh.databasewrapper.exception.DatabaseWrapperException;
import in.asvignesh.databasewrapper.utils.DatabaseUtils;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import javax.sql.DataSource;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.sql2o.Sql2o;
import org.sql2o.converters.Converter;
import org.sql2o.quirks.Quirks;
import org.sql2o.quirks.QuirksDetector;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DatabaseWrapper {

  private static DatabaseWrapper instance;
  /**
   * The object of the underlying operation database.
   */
  @Getter
  @Setter
  private Sql2o sql2o;
  /**
   * Global table prefix
   */
  private String tablePrefix;
  /**
   * Database dialect, default by MySQL
   */
  private Dialect dialect = new MySQLDialect();
  /**
   * The type of rollback when an exception occurs, default by RuntimeException
   */
  private Class<? extends Exception> rollbackException = RuntimeException.class;
  /**
   * SQL performance statistics are enabled, which is enabled by default, and outputs the elapsed
   * time required.
   */
  private boolean enableSQLStatistic = true;
  /**
   * use the limit statement of SQL and use "limit ?" when enabled, the way to retrieve a fixed
   * number of rows.
   */
  private boolean useSQLLimit = true;

  /**
   * Create DatabaseWrapper with Sql2o
   *
   * @param sql2o sql2o instance
   */
  public DatabaseWrapper(Sql2o sql2o) {
    open(sql2o);
  }

  /**
   * Create DatabaseWrapper with datasource
   *
   * @param dataSource datasource instance
   */
  public DatabaseWrapper(DataSource dataSource) {
    open(dataSource);
  }

  /**
   * Create DatabaseWrapper with url and db info
   *
   * @param url jdbc url
   * @param user database username
   * @param pass database password
   */
  public DatabaseWrapper(String url, String user, String pass) {
    open(url, user, pass);
  }

  /**
   * see {@link #of()}
   */
  @Deprecated
  public static DatabaseWrapper me() {
    return of();
  }

  public static DatabaseWrapper of() {
    return ifReturnOrThrow(null != instance && null != instance.sql2o,
        instance,
        new DatabaseWrapperException(SQL2O_IS_NULL));
  }

  /**
   * Create DatabaseWrapper with Sql2o
   *
   * @param sql2o sql2o instance
   * @return DatabaseWrapper
   */
  public static DatabaseWrapper open(Sql2o sql2o) {
    DatabaseWrapper databaseWrapper = new DatabaseWrapper();
    databaseWrapper.setSql2o(sql2o);
    instance = databaseWrapper;
    return databaseWrapper;
  }

  /**
   * Create DatabaseWrapper with url, like Sqlite or h2
   *
   * @param url jdbc url
   * @return DatabaseWrapper
   */
  public static DatabaseWrapper open(String url) {
    return open(url, null, null);
  }

  /**
   * Create DatabaseWrapper with url, like Sqlite or h2
   *
   * @param url jdbc url
   * @return DatabaseWrapper
   */
  public static DatabaseWrapper open(String url, Quirks quirks) {
    return open(url, null, null, quirks);
  }

  /**
   * Create DatabaseWrapper with datasource
   *
   * @param dataSource datasource instance
   * @return DatabaseWrapper
   */
  public static DatabaseWrapper open(DataSource dataSource) {
    return open(new Sql2o(dataSource));
  }

  /**
   * Create DatabaseWrapper with datasource and quirks
   *
   * @param dataSource datasource instance
   * @return DatabaseWrapper
   */
  public static DatabaseWrapper open(DataSource dataSource, Quirks quirks) {
    return open(new Sql2o(dataSource, quirks));
  }

  /**
   * Create DatabaseWrapper with url and db info
   *
   * @param url jdbc url
   * @param user database username
   * @param pass database password
   * @return DatabaseWrapper
   */
  public static DatabaseWrapper open(String url, String user, String pass) {
    return open(url, user, pass, QuirksDetector.forURL(url));
  }

  /**
   * Create DatabaseWrapper with url and db info
   *
   * @param url jdbc url
   * @param user database username
   * @param pass database password
   * @param quirks DBQuirks
   * @return DatabaseWrapper
   */
  public static DatabaseWrapper open(String url, String user, String pass, Quirks quirks) {
    return open(new Sql2o(url, user, pass, quirks));
  }

  /**
   * Code that performs a transaction operation.
   *
   * @param runnable the code snippet to execute.
   * @return Atomic
   */
  public static Atomic atomic(Runnable runnable) {
    try {
      Query.beginTransaction();
      runnable.run();
      Query.commit();
      return Atomic.ok();
    } catch (Exception e) {

      boolean isRollback = ifReturn(
          of().rollbackException.isInstance(e),
          () -> {
            Query.rollback();
            return true;
          },
          () -> false);

      return Atomic.error(e).rollback(isRollback);
    } finally {
      Query.endTransaction();
    }
  }

  /**
   * Open a query statement.
   *
   * @return Select
   */
  public static Select select() {
    return new Select();
  }

  /**
   * Open a query statement and specify the query for some columns.
   *
   * @param columns column names
   * @return Select
   */
  public static Select select(String columns) {
    return new Select(columns);
  }

  /**
   * Set the query to fix columns with lambda
   *
   * @param functions column lambdas
   * @return Select
   */
  @SafeVarargs
  public static <T extends DataModel, R> Select select(TypeFunction<T, R>... functions) {
    return select(
        Arrays.stream(functions)
            .map(DatabaseUtils::getLambdaColumnName)
            .collect(joining(", ")));
  }

  /**
   * Open an update statement.
   *
   * @return Update
   */
  public static Update update() {
    return new Update();
  }

  /**
   * Open a delete statement.
   *
   * @return Delete
   */
  public static Delete delete() {
    return new Delete();
  }

  /**
   * Save a model
   *
   * @param model database model
   * @return ResultKey
   */
  public static <T extends DataModel> ResultKey save(T model) {
    return model.save();
  }

  /**
   * Batch save model
   *
   * @param models model list
   */
  public static <T extends DataModel> void saveBatch(List<T> models) {
    atomic(() -> models.forEach(DatabaseWrapper::save))
        .catchException(e -> System.out.println("Batch save model error, message: {}" + e));
  }

  /**
   * Batch delete model
   *
   * @param model model class type
   * @param ids mode primary id array
   */
  @SafeVarargs
  public static <T extends DataModel, S extends Serializable> void deleteBatch(
      Class<T> model,
      S... ids) {
    atomic(() -> Arrays.stream(ids)
        .forEach(new Query<>(model)::deleteById))
        .catchException(e -> System.out.println("Batch save model error, message: {}" + e));
  }

  /**
   * Batch delete model with List
   *
   * @param model model class type
   * @param idList mode primary id list
   */
  public static <T extends DataModel, S extends Serializable> void deleteBatch(
      Class<T> model,
      List<S> idList) {
    deleteBatch(model, DatabaseUtils.toArray(idList));
  }

  /**
   * Delete model by id
   *
   * @param model model type class
   * @param id model primary key
   */
  public static <T extends DataModel> int deleteById(Class<T> model, Serializable id) {
    return new Query<>(model).deleteById(id);
  }

  /**
   * Execute SQL statement
   *
   * @param sql sql statement
   * @param params params
   * @return number of rows affected after execution
   */
  public static int execute(String sql, Object... params) {
    return new Query<>().execute(sql, params);
  }

  /**
   * Set the type of rollback exception to trigger the transaction rollback.
   *
   * @param rollbackException roll back exception type
   * @return DatabaseWrapper
   */
  public DatabaseWrapper rollbackException(Class<? extends Exception> rollbackException) {
    this.rollbackException = rollbackException;
    return this;
  }

  public Class<? extends Exception> rollbackException() {
    return this.rollbackException;
  }

  /**
   * Set the global table prefix, like "t_"
   *
   * @param tablePrefix table prefix
   * @return DatabaseWrapper
   */
  public DatabaseWrapper tablePrefix(String tablePrefix) {
    this.tablePrefix = tablePrefix;
    return this;
  }

  public String tablePrefix() {
    return this.tablePrefix;
  }

  /**
   * Specify a database dialect.
   *
   * @param dialect @see Dialect
   * @return DatabaseWrapper
   */
  public DatabaseWrapper dialect(Dialect dialect) {
    this.dialect = dialect;
    return this;
  }

  public Dialect dialect() {
    return this.dialect;
  }

  /**
   * Set whether SQL statistics are enabled.
   *
   * @param enableSQLStatistic sql statistics
   * @return DatabaseWrapper
   */
  public DatabaseWrapper enableSQLStatistic(boolean enableSQLStatistic) {
    this.enableSQLStatistic = enableSQLStatistic;
    return this;
  }

  public boolean isEnableSQLStatistic() {
    return this.enableSQLStatistic;
  }

  /**
   * Set the use of SQL limit.
   *
   * @param useSQLLimit use sql limit
   * @return DatabaseWrapper
   */
  public DatabaseWrapper useSQLLimit(boolean useSQLLimit) {
    this.useSQLLimit = useSQLLimit;
    return this;
  }

  public boolean isUseSQLLimit() {
    return this.useSQLLimit;
  }

  /**
   * Add custom Type converter
   *
   * @param converters converter see {@link Converter}
   * @return DatabaseWrapper
   */
  public DatabaseWrapper addConverter(Converter<?>... converters) {
    ifThrow(null == converters || converters.length == 0,
        new DatabaseWrapperException("converters not be null."));

    for (Converter<?> converter : converters) {
      Class<?> type = DatabaseUtils.getConverterType(converter);
//            sql2o.getQuirks().addConverter(type, converter);
    }
    return this;
  }


}