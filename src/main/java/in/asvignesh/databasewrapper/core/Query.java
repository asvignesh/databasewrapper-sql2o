package in.asvignesh.databasewrapper.core;

import static in.asvignesh.databasewrapper.core.DatabaseCache.computeModelColumnMappings;
import static in.asvignesh.databasewrapper.core.DatabaseCache.getGetterName;
import static in.asvignesh.databasewrapper.core.DatabaseCache.getSetterName;
import static in.asvignesh.databasewrapper.utils.Functions.ifNotNullReturn;
import static in.asvignesh.databasewrapper.utils.Functions.ifNotNullThen;
import static in.asvignesh.databasewrapper.utils.Functions.ifNullThen;
import static in.asvignesh.databasewrapper.utils.Functions.ifNullThrow;
import static in.asvignesh.databasewrapper.utils.Functions.ifReturn;
import static in.asvignesh.databasewrapper.utils.Functions.ifThen;
import static java.util.stream.Collectors.toList;

import in.asvignesh.databasewrapper.DataModel;
import in.asvignesh.databasewrapper.DatabaseWrapper;
import in.asvignesh.databasewrapper.core.functions.TypeFunction;
import in.asvignesh.databasewrapper.enums.DMLType;
import in.asvignesh.databasewrapper.enums.ErrorCode;
import in.asvignesh.databasewrapper.enums.OrderBy;
import in.asvignesh.databasewrapper.exception.DatabaseWrapperException;
import in.asvignesh.databasewrapper.page.Page;
import in.asvignesh.databasewrapper.page.PageRow;
import in.asvignesh.databasewrapper.utils.DatabaseUtils;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.NoArgsConstructor;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

@NoArgsConstructor
public class Query<T extends DataModel> {

  private static Sql2o sql2o;

  private static ThreadLocal<Connection> localConnection = new ThreadLocal<>();

  private Class<T> modelClass;

  private StringBuilder conditionSQL = new StringBuilder();


  private StringBuilder orderBySQL = new StringBuilder();


  private List<String> excludedColumns = new ArrayList<>(8);


  private List<Object> paramValues = new ArrayList<>(8);


  private Map<String, Object> updateColumns = new LinkedHashMap<>(8);


  private boolean isSQLLimit;


  private boolean useSQL;


  private String selectColumns;


  private String primaryKeyColumn;


  private String tableName;


  private DMLType dmlType;


  private List<JoinParam> joinParams = new ArrayList<>();

  public Query(DMLType dmlType) {
    this.dmlType = dmlType;
  }

  public Query(Class<T> modelClass) {
    this.parse(modelClass);
  }


  public static void beginTransaction() {
    ifNullThen(localConnection.get(),
        () -> {
          Connection connection = getSql2o().beginTransaction();
          localConnection.set(connection);
        });
  }


  public static void endTransaction() {
    ifNotNullThen(localConnection.get(),
        () -> {
          Connection connection = localConnection.get();
          ifThen(connection.isRollbackOnClose(), connection::close);
          localConnection.remove();
        });
  }


  public static void commit() {
    localConnection.get().commit();
  }


  public static void rollback() {
    ifNotNullThen(localConnection.get(),
        () -> {
//          log.error("Rollback connection.");
          localConnection.get().rollback();
        });
  }

  public static Sql2o getSql2o() {
    return ifNotNullReturn(sql2o,
        () -> {
          Sql2o sql2o = DatabaseWrapper.of().getSql2o();
          ifNullThrow(sql2o, new DatabaseWrapperException("SQL2O instance not is null."));
          return sql2o;
        });
  }

  public Query<T> parse(Class<T> modelClass) {
    this.modelClass = modelClass;
    this.tableName = DatabaseCache.getTableName(modelClass);
    this.primaryKeyColumn = DatabaseCache.getPKColumn(modelClass);
    return this;
  }


  public Query<T> exclude(String... columnNames) {
    Collections.addAll(excludedColumns, columnNames);
    return this;
  }

  public <R> Query<T> exclude(TypeFunction<T, R>... functions) {
    String[] columnNames = Arrays.stream(functions)
        .map(DatabaseUtils::getLambdaColumnName)
        .collect(toList())
        .toArray(new String[functions.length]);
    return this.exclude(columnNames);
  }

  public Query<T> select(String columns) {
    if (null != this.selectColumns) {
      throw new DatabaseWrapperException("Select method can only be called once.");
    }
    this.selectColumns = columns;
    return this;
  }

  public Query<T> where(String statement) {
    conditionSQL.append(" AND ").append(statement);
    return this;
  }

  public Query<T> where(String statement, Object value) {
    conditionSQL.append(" AND ").append(statement);
    if (!statement.contains("?")) {
      conditionSQL.append(" = ?");
    }
    paramValues.add(value);
    return this;
  }

  public <R> Query<T> where(TypeFunction<T, R> function) {
    String columnName = DatabaseUtils.getLambdaColumnName(function);
    conditionSQL.append(" AND ").append(columnName);
    return this;
  }

  public <S extends DataModel, R> Query<T> where(TypeFunction<S, R> function,
      Object value) {
    String columnName = DatabaseUtils.getLambdaColumnName(function);
    conditionSQL.append(" AND ").append(columnName).append(" = ?");
    paramValues.add(value);
    return this;
  }

  public Query<T> where(T model) {
    Field[] declaredFields = model.getClass().getDeclaredFields();
    for (Field declaredField : declaredFields) {

      Object value = DatabaseUtils
          .invokeMethod(model, getGetterName(declaredField.getName()),
              DatabaseUtils.EMPTY_ARG);
      if (null == value) {
        continue;
      }
      if (declaredField.getType().equals(String.class) && DatabaseUtils
          .isEmpty(value.toString())) {
        continue;
      }
      String columnName = DatabaseCache.getColumnName(declaredField);
      this.where(columnName, value);
    }
    return this;
  }

  public Query<T> eq(Object value) {
    conditionSQL.append(" = ?");
    paramValues.add(value);
    return this;
  }

  public Query<T> notNull() {
    conditionSQL.append(" IS NOT NULL");
    return this;
  }

  public Query<T> and(String statement, Object value) {
    return this.where(statement, value);
  }

  public <R> Query<T> and(TypeFunction<T, R> function) {
    return this.where(function);
  }

  public <R> Query<T> and(TypeFunction<T, R> function, Object value) {
    return this.where(function, value);
  }

  public Query<T> or(String statement, Object value) {
    conditionSQL.append(" OR (").append(statement);
    if (!statement.contains("?")) {
      conditionSQL.append(" = ?");
    }
    conditionSQL.append(')');
    paramValues.add(value);
    return this;
  }

  public Query<T> notEq(String columnName, Object value) {
    conditionSQL.append(" AND ").append(columnName).append(" != ?");
    paramValues.add(value);
    return this;
  }

  public <R> Query<T> notEq(TypeFunction<T, R> function, Object value) {
    String columnName = DatabaseUtils.getLambdaColumnName(function);
    return this.notEq(columnName, value);
  }

  public Query<T> notEq(Object value) {
    conditionSQL.append(" != ?");
    paramValues.add(value);
    return this;
  }

  public Query<T> notEmpty(String columnName) {
    conditionSQL.append(" AND ").append(columnName).append(" != ''");
    return this;
  }

  public <R> Query<T> notEmpty(TypeFunction<T, R> function) {
    String columnName = DatabaseUtils.getLambdaColumnName(function);
    return this.notEmpty(columnName);
  }

  public Query<T> notEmpty() {
    conditionSQL.append(" != ''");
    return this;
  }


  public Query<T> notNull(String columnName) {
    conditionSQL.append(" AND ").append(columnName).append(" IS NOT NULL");
    return this;
  }


  public Query<T> like(String columnName, Object value) {
    conditionSQL.append(" AND ").append(columnName).append(" LIKE ?");
    paramValues.add(value);
    return this;
  }


  public <R> Query<T> like(TypeFunction<T, R> function, Object value) {
    String columnName = DatabaseUtils.getLambdaColumnName(function);
    return this.like(columnName, value);
  }


  public Query<T> like(Object value) {
    conditionSQL.append(" LIKE ?");
    paramValues.add(value);
    return this;
  }


  public Query<T> between(String columnName, Object a, Object b) {
    conditionSQL.append(" AND ").append(columnName).append(" BETWEEN ? and ?");
    paramValues.add(a);
    paramValues.add(b);
    return this;
  }


  public <R> Query<T> between(TypeFunction<T, R> function, Object a, Object b) {
    String columnName = DatabaseUtils.getLambdaColumnName(function);
    return this.between(columnName, a, b);
  }


  public Query<T> between(Object a, Object b) {
    conditionSQL.append(" BETWEEN ? and ?");
    paramValues.add(a);
    paramValues.add(b);
    return this;
  }

  public Query<T> gt(String columnName, Object value) {
    conditionSQL.append(" AND ").append(columnName).append(" > ?");
    paramValues.add(value);
    return this;
  }

  public <R> Query<T> gt(TypeFunction<T, R> function, Object value) {
    String columnName = DatabaseUtils.getLambdaColumnName(function);
    return this.gt(columnName, value);
  }

  public Query<T> gt(Object value) {
    conditionSQL.append(" > ?");
    paramValues.add(value);
    return this;
  }

  public Query<T> gte(Object value) {
    conditionSQL.append(" >= ?");
    paramValues.add(value);
    return this;
  }

  public <S extends DataModel, R> Query<T> gte(TypeFunction<S, R> function,
      Object value) {
    String columnName = DatabaseUtils.getLambdaColumnName(function);
    return this.gte(columnName, value);
  }


  public Query<T> lt(Object value) {
    conditionSQL.append(" < ?");
    paramValues.add(value);
    return this;
  }


  public <S extends DataModel, R> Query<T> lt(TypeFunction<S, R> function,
      Object value) {
    String columnName = DatabaseUtils.getLambdaColumnName(function);
    return this.lt(columnName, value);
  }


  public Query<T> lte(Object value) {
    conditionSQL.append(" <= ?");
    paramValues.add(value);
    return this;
  }


  public <S extends DataModel, R> Query<T> lte(TypeFunction<S, R> function,
      Object value) {
    String columnName = DatabaseUtils.getLambdaColumnName(function);
    return this.lte(columnName, value);
  }


  public Query<T> gte(String column, Object value) {
    conditionSQL.append(" AND ").append(column).append(" >= ?");
    paramValues.add(value);
    return this;
  }

  public Query<T> lt(String column, Object value) {
    conditionSQL.append(" AND ").append(column).append(" < ?");
    paramValues.add(value);
    return this;
  }


  public Query<T> lte(String column, Object value) {
    conditionSQL.append(" AND ").append(column).append(" <= ?");
    paramValues.add(value);
    return this;
  }


  public Query<T> in(String column, Object... args) {
    if (null == args || args.length == 0) {
//      log.warn("Column: {}, query params is empty.");
      return this;
    }
    conditionSQL.append(" AND ").append(column).append(" IN (");
    this.setArguments(args);
    conditionSQL.append(")");
    return this;
  }


  public Query<T> in(Object... args) {
    if (null == args || args.length == 0) {
//      log.warn("Column: {}, query params is empty.");
      return this;
    }
    conditionSQL.append(" IN (");
    this.setArguments(args);
    conditionSQL.append(")");
    return this;
  }


  public <S> Query<T> in(List<S> list) {
    return this.in(list.toArray());
  }


  public <S> Query<T> in(String column, List<S> args) {
    return this.in(column, args.toArray());
  }


  public <R> Query<T> in(TypeFunction<T, R> function, Object... values) {
    String columnName = DatabaseUtils.getLambdaColumnName(function);
    return this.in(columnName, values);
  }


  public <S, R> Query<T> in(TypeFunction<T, R> function, List<S> values) {
    String columnName = DatabaseUtils.getLambdaColumnName(function);
    return this.in(columnName, values);
  }


  public Query<T> order(String order) {
    if (this.orderBySQL.length() > 0) {
      this.orderBySQL.append(',');
    }
    this.orderBySQL.append(' ').append(order);
    return this;
  }


  public Query<T> order(String columnName, OrderBy orderBy) {
    if (this.orderBySQL.length() > 0) {
      this.orderBySQL.append(',');
    }
    this.orderBySQL.append(' ').append(columnName).append(' ').append(orderBy.toString());
    return this;
  }


  public <R> Query<T> order(TypeFunction<T, R> function, OrderBy orderBy) {
    String columnName = DatabaseUtils.getLambdaColumnName(function);
    return order(columnName, orderBy);
  }


  public T byId(Object id) {
    this.beforeCheck();
    this.where(primaryKeyColumn, id);

    String sql = this.buildSelectSQL(false);

    T model = this.queryOne(modelClass, sql, paramValues);

    ifNotNullThen(model, () -> this.setJoin(Collections.singletonList(model)));

    return model;
  }


  public List<T> byIds(Object... ids) {
    this.in(this.primaryKeyColumn, ids);
    return this.all();
  }


  public T one() {
    this.beforeCheck();

    String sql = this.buildSelectSQL(true);

    T model = this.queryOne(modelClass, sql, paramValues);

    ifThen(null != model && null != joinParams,
        () -> this.setJoin(Collections.singletonList(model)));

    return model;
  }


  public List<T> all() {
    this.beforeCheck();
    String sql = this.buildSelectSQL(true);
    List<T> models = this.queryList(modelClass, sql, paramValues);
    this.setJoin(models);
    return models;
  }


  public List<Map<String, Object>> maps() {
    this.beforeCheck();
    String sql = this.buildSelectSQL(true);
    return this.queryListMap(sql, paramValues);
  }


  public Stream<T> stream() {
    List<T> all = all();

    return ifReturn(null == all || all.isEmpty(),
        Stream.empty(),
        Objects.requireNonNull(all).stream());
  }


  public Stream<T> parallel() {
    return stream().parallel();
  }


  public <R> Stream<R> map(Function<T, R> function) {
    return stream().map(function);
  }


  public Stream<T> filter(Predicate<T> predicate) {
    return stream().filter(predicate);
  }


  public List<T> limit(int limit) {
    return ifReturn(DatabaseWrapper.of().isUseSQLLimit(),
        () -> {
          isSQLLimit = true;
          paramValues.add(limit);
          return all();
        },
        () -> {
          List<T> all = all();
          return ifReturn(all.size() > limit,
              all.stream().limit(limit).collect(toList()),
              all);
        });
  }


  public Page<T> page(int page, int limit) {
    return this.page(new PageRow(page, limit));
  }


  public Page<T> page(String sql, PageRow pageRow) {
    return this.page(sql, paramValues, pageRow);
  }


  public Page<T> page(String sql, List<Object> paramValues, PageRow pageRow) {
    return this.page(sql, paramValues.toArray(), pageRow);
  }


  public Page<T> page(String sql, Object[] params, PageRow pageRow) {
    this.beforeCheck();
    Connection conn = getConn();
    try {
      String countSql = useSQL ? "SELECT COUNT(*) FROM (" + sql + ") tmp" : buildCountSQL(sql);

      long count = conn.createQuery(countSql)
          .withParams(params)
          .executeAndFetchFirst(Long.class);

      Page<T> pageBean = new Page<>(count, pageRow.getPageNum(), pageRow.getPageSize());

      ifThen(count > 0, () -> {
        String pageSQL = this.buildPageSQL(sql, pageRow);
        List<T> list = conn.createQuery(pageSQL)
            .withParams(params)
            .setAutoDeriveColumnNames(true)
            .throwOnMappingFailure(false)
            .executeAndFetch(modelClass);

        this.setJoin(list);
        pageBean.setRows(list);
      });

      return pageBean;
    } finally {
      this.closeConn(conn);
      this.clean(null);
    }
  }

  private String buildCountSQL(String sql) {
    return "SELECT COUNT(*) " + sql.substring(sql.indexOf("FROM"));
  }


  public Page<T> page(PageRow pageRow) {
    String sql = this.buildSelectSQL(false);
    return this.page(sql, pageRow);
  }


  public long count() {
    this.beforeCheck();
    String sql = this.buildCountSQL();
    return this.queryOne(Long.class, sql, paramValues);
  }


  public Query<T> set(String column, Object value) {
    updateColumns.put(column, value);
    return this;
  }


  public <S extends DataModel, R> Query<T> set(TypeFunction<S, R> function,
      Object value) {
    return this.set(DatabaseUtils.getLambdaColumnName(function), value);
  }

  public Query<T> join(JoinParam joinParam) {
    ifNullThrow(joinParam,
        new DatabaseWrapperException("Join param not null"));

    ifNullThrow(joinParam.getJoinModel(),
        new DatabaseWrapperException("Join param [model] not null"));

    ifNullThrow(DatabaseUtils.isEmpty(joinParam.getFieldName()),
        new DatabaseWrapperException("Join param [as] not empty"));

    ifNullThrow(DatabaseUtils.isEmpty(joinParam.getOnLeft()),
        new DatabaseWrapperException("Join param [onLeft] not empty"));

    ifNullThrow(DatabaseUtils.isEmpty(joinParam.getOnRight()),
        new DatabaseWrapperException("Join param [onRight] not empty"));

    this.joinParams.add(joinParam);
    return this;
  }

  public <S> S queryOne(Class<S> type, String sql, Object[] params) {
    Connection conn = getConn();
    try {
      org.sql2o.Query query = conn.createQuery(sql)
          .withParams(params)
          .setAutoDeriveColumnNames(true)
          .throwOnMappingFailure(false);

      return ifReturn(DatabaseUtils.isBasicType(type),
          () -> query.executeScalar(type),
          () -> query.executeAndFetchFirst(type));
    } finally {
      this.closeConn(conn);
      this.clean(null);
    }
  }

  public <S> S queryOne(Class<S> type, String sql, List<Object> params) {
    if (DatabaseWrapper.of().isUseSQLLimit()) {
      sql += " LIMIT 1";
    }
    List<S> list = queryList(type, sql, params);
    return DatabaseUtils.isNotEmpty(list) ? list.get(0) : null;
  }


  public <S> List<S> queryList(Class<S> type, String sql, Object[] params) {
    Connection conn = getConn();
    try {
      return conn.createQuery(sql)
          .withParams(params)
          .setColumnMappings(computeModelColumnMappings(type))
          .throwOnMappingFailure(false)
          .executeAndFetch(type);
    } finally {
      this.closeConn(conn);
      this.clean(null);
    }
  }


  public <S> List<S> queryList(Class<S> type, String sql, List<Object> params) {
    return this.queryList(type, sql, params.toArray());
  }

  public List<Map<String, Object>> queryListMap(String sql, Object[] params) {
    Connection conn = getConn();
    try {
      return conn.createQuery(sql)
          .withParams(params)
          .setAutoDeriveColumnNames(true)
          .throwOnMappingFailure(false)
          .executeAndFetchTable()
          .asList();
    } finally {
      this.closeConn(conn);
      this.clean(null);
    }
  }

  public List<Map<String, Object>> queryListMap(String sql, List<Object> params) {
    return this.queryListMap(sql, params.toArray());
  }


  public int execute() {
    switch (dmlType) {
      case UPDATE:
        return this.update();
      case DELETE:
        return this.delete();
      default:
        throw new DatabaseWrapperException("Please check if your use is correct.");
    }
  }


  public int execute(String sql, Object... params) {
    Connection conn = getConn();
    try {
      int pos = 1;
      while (sql.contains("?")) {
        sql = sql.replaceFirst("\\?", ":p" + (pos++));
      }
      params = params == null ? new Object[]{} : params;
      return conn.createQuery(sql)
          .withParams(params)
          .executeUpdate()
          .getResult();
    } finally {
      this.closeConn(conn);
      this.clean(conn);
    }
  }

  public Object executeAndGetKey(String sql, Object... params) {
    Connection conn = getConn();
    try {
      int pos = 1;
      while (sql.contains("?")) {
        sql = sql.replaceFirst("\\?", ":p" + (pos++));
      }
      params = params == null ? new Object[]{} : params;
      return conn.createQuery(sql)
          .withParams(params)
          .executeUpdate()
          .getKey();
    } finally {
      this.closeConn(conn);
      this.clean(conn);
    }
  }


  public int execute(String sql, List<Object> params) {
    return this.execute(sql, params.toArray());
  }

  public Object executeAndGetKey(String sql, List<Object> params) {
    return this.executeAndGetKey(sql, params.toArray());
  }


  public <S extends DataModel> ResultKey save(S model) {
    List<Object> columnValues = DatabaseUtils.toColumnValues(model, true);
    String sql = this.buildInsertSQL(model, columnValues);
    Connection conn = getConn();
    try {

      List<Object> params = columnValues.stream()
          .filter(Objects::nonNull)
          .collect(toList());

      int pos = 1;
      while (sql.contains("?")) {
        sql = sql.replaceFirst("\\?", ":p" + (pos++));
      }
      return new ResultKey(executeAndGetKey(sql, params));
    } finally {
      this.closeConn(conn);
      this.clean(conn);
    }
  }

  public <S extends DataModel> ResultKey saveOrUpdateOnDuplicate(S model) {
    List<Object> columnValues = DatabaseUtils.toColumnValues(model, true);
    List<Object> duplicateColumnValues = DatabaseUtils.toColumnValuesDuplicate(model, true);
    String sql = this.buildInsertOrUpdateOnDuplicateSQL(model, columnValues);
    Connection conn = getConn();
    try {

      List<Object> params = columnValues.stream()
          .filter(Objects::nonNull)
          .collect(toList());

      List<Object> duplicateParams = duplicateColumnValues.stream()
          .filter(Objects::nonNull)
          .collect(toList());

      params.addAll(duplicateParams);

      int pos = 1;
      while (sql.contains("?")) {
        sql = sql.replaceFirst("\\?", ":p" + (pos++));
      }

      return new ResultKey(executeAndGetKey(sql, params));
//          new ResultKey(conn.createQuery(sql)
//          .withParams(params)
//          .executeUpdate()
//          .getKey());
    } finally {
      this.closeConn(conn);
      this.clean(conn);
    }
  }

  public int delete() {
    String sql = this.buildDeleteSQL(null);
    return this.execute(sql, paramValues);
  }


  public <S extends Serializable> int deleteById(S id) {
    this.where(primaryKeyColumn, id);
    return this.delete();
  }

  public <S extends DataModel> int deleteByModel(S model) {
    this.beforeCheck();
    String sql = this.buildDeleteSQL(model);
    List<Object> columnValueList = DatabaseUtils.toColumnValues(model, false);
    return this.execute(sql, columnValueList);
  }


  public int update() {
    this.beforeCheck();
    String sql = this.buildUpdateSQL(null, updateColumns);
    List<Object> columnValueList = new ArrayList<>();
    updateColumns.forEach((key, value) -> columnValueList.add(value));
    columnValueList.addAll(paramValues);
    return this.execute(sql, columnValueList);
  }


  public int updateById(Serializable id) {
    this.where(primaryKeyColumn, id);
    return this.update();
  }


  public <S extends DataModel> int updateById(S model, Serializable id) {
    this.where(primaryKeyColumn, id);
    String sql = this.buildUpdateSQL(model, null);
    List<Object> columnValueList = DatabaseUtils.toColumnValues(model, false);
    columnValueList.add(id);
    return this.execute(sql, columnValueList);
  }


  public <S extends DataModel> int updateByModel(S model) {
    this.beforeCheck();

    Object primaryKey = DatabaseUtils.getAndRemovePrimaryKey(model);

    StringBuilder sql = new StringBuilder(this.buildUpdateSQL(model, null));

    List<Object> columnValueList = DatabaseUtils.toColumnValues(model, false);

    ifNotNullThen(primaryKey, () -> {
      sql.append(" WHERE ").append(this.primaryKeyColumn).append(" = ?");
      columnValueList.add(primaryKey);
    });

    return this.execute(sql.toString(), columnValueList);
  }

  private void setArguments(Object[] args) {
    for (int i = 0; i < args.length; i++) {

      ifThen(i == args.length - 1,
          () -> conditionSQL.append("?"),
          () -> conditionSQL.append("?, "));

      paramValues.add(args[i]);
    }
  }


  private String buildSelectSQL(boolean addOrderBy) {
    SQLParams sqlParams = SQLParams.builder()
        .modelClass(this.modelClass)
        .selectColumns(this.selectColumns)
        .tableName(this.tableName)
        .pkName(this.primaryKeyColumn)
        .conditionSQL(this.conditionSQL)
        .excludedColumns(this.excludedColumns)
        .isSQLLimit(isSQLLimit)
        .build();

    ifThen(addOrderBy, () -> sqlParams.setOrderBy(this.orderBySQL.toString()));

    return DatabaseWrapper.of().dialect().select(sqlParams);
  }


  private String buildCountSQL() {
    SQLParams sqlParams = SQLParams.builder()
        .modelClass(this.modelClass)
        .tableName(this.tableName)
        .pkName(this.primaryKeyColumn)
        .conditionSQL(this.conditionSQL)
        .build();
    return DatabaseWrapper.of().dialect().count(sqlParams);
  }


  private String buildPageSQL(String sql, PageRow pageRow) {
    SQLParams sqlParams = SQLParams.builder()
        .modelClass(this.modelClass)
        .selectColumns(this.selectColumns)
        .tableName(this.tableName)
        .pkName(this.primaryKeyColumn)
        .conditionSQL(this.conditionSQL)
        .excludedColumns(this.excludedColumns)
        .customSQL(sql)
        .orderBy(this.orderBySQL.toString())
        .pageRow(pageRow)
        .build();
    return DatabaseWrapper.of().dialect().paginate(sqlParams);
  }

  private <S extends DataModel> String buildInsertSQL(S model,
      List<Object> columnValues) {
    SQLParams sqlParams = SQLParams.builder()
        .model(model)
        .columnValues(columnValues)
        .modelClass(this.modelClass)
        .tableName(this.tableName)
        .pkName(this.primaryKeyColumn)
        .build();

    return DatabaseWrapper.of().dialect().insert(sqlParams);
  }

  private <S extends DataModel> String buildInsertOrUpdateOnDuplicateSQL(S model,
      List<Object> columnValues) {
    SQLParams sqlParams = SQLParams.builder()
        .model(model)
        .columnValues(columnValues)
        .modelClass(this.modelClass)
        .tableName(this.tableName)
        .pkName(this.primaryKeyColumn)
        .build();

    return DatabaseWrapper.of().dialect().insertOnDuplicate(sqlParams);
  }

  private <S extends DataModel> String buildUpdateSQL(S model,
      Map<String, Object> updateColumns) {
    SQLParams sqlParams = SQLParams.builder()
        .model(model)
        .modelClass(this.modelClass)
        .tableName(this.tableName)
        .pkName(this.primaryKeyColumn)
        .updateColumns(updateColumns)
        .conditionSQL(this.conditionSQL)
        .build();

    return DatabaseWrapper.of().dialect().update(sqlParams);
  }

  private <S extends DataModel> String buildDeleteSQL(S model) {
    SQLParams sqlParams = SQLParams.builder()
        .model(model)
        .modelClass(this.modelClass)
        .tableName(this.tableName)
        .pkName(this.primaryKeyColumn)
        .conditionSQL(this.conditionSQL)
        .build();
    return DatabaseWrapper.of().dialect().delete(sqlParams);
  }

  public Query<T> useSQL() {
    this.useSQL = true;
    return this;
  }


  private void beforeCheck() {
    ifNullThrow(this.modelClass, new DatabaseWrapperException(ErrorCode.FROM_NOT_NULL));
  }

  private Connection getConn() {
    Connection connection = localConnection.get();
    return ifNotNullReturn(connection, connection, getSql2o().open());
  }

  public Query<T> bindSQL2o(Sql2o sql2o) {
    Query.sql2o = sql2o;
    return this;
  }

  private void setJoin(List<T> models) {
    if (null == models || models.isEmpty() ||
        joinParams.size() == 0) {
      return;
    }
    models.stream().filter(Objects::nonNull).forEach(this::setJoin);
  }

  private void setJoin(T model) {
    for (JoinParam joinParam : joinParams) {
      try {
        Object leftValue = DatabaseUtils.invokeMethod(
            model,
            getGetterName(joinParam.getOnLeft()),
            DatabaseUtils.EMPTY_ARG);

        String sql = "SELECT * FROM " + DatabaseCache.getTableName(joinParam.getJoinModel()) +
            " WHERE " + joinParam.getOnRight() + " = ?";

        Field field = model.getClass()
            .getDeclaredField(joinParam.getFieldName());

        if (field.getType().equals(List.class)) {
          if (DatabaseUtils.isNotEmpty(joinParam.getOrderBy())) {
            sql += " ORDER BY " + joinParam.getOrderBy();
          }
          List<? extends DataModel> list = this
              .queryList(joinParam.getJoinModel(), sql, new Object[]{leftValue});
          DatabaseUtils
              .invokeMethod(model, getSetterName(joinParam.getFieldName()), new Object[]{list});
        }

        if (field.getType().equals(joinParam.getJoinModel())) {
          Object joinObject = this.queryOne(joinParam.getJoinModel(), sql, new Object[]{leftValue});
          DatabaseUtils.invokeMethod(model, getSetterName(joinParam.getFieldName()),
              new Object[]{joinObject});
        }
      } catch (NoSuchFieldException e) {
//        log.error("Set join error", e);
      }
    }
  }

  private void closeConn(Connection connection) {
    ifThen(localConnection.get() == null && connection != null,
        () -> connection.close());
  }

  private void clean(Connection conn) {
    this.selectColumns = null;
    this.isSQLLimit = false;
    this.orderBySQL = new StringBuilder();
    this.conditionSQL = new StringBuilder();
    this.paramValues.clear();
    this.excludedColumns.clear();
    this.updateColumns.clear();

    ifThen(localConnection.get() == null && conn != null,
        () -> conn.close());
  }

}
