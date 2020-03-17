package in.asvignesh.databasewrapper.dialect;

import static in.asvignesh.databasewrapper.core.DatabaseCache.getGetterName;
import static in.asvignesh.databasewrapper.utils.Functions.ifThen;

import in.asvignesh.databasewrapper.core.DatabaseCache;
import in.asvignesh.databasewrapper.core.SQLParams;
import in.asvignesh.databasewrapper.exception.DatabaseWrapperException;
import in.asvignesh.databasewrapper.utils.DatabaseUtils;
import java.lang.reflect.Field;
import java.util.List;

public interface Dialect {

  default String select(SQLParams sqlParams) {
    StringBuilder sql = new StringBuilder();
    if (DatabaseUtils.isNotEmpty(sqlParams.getCustomSQL())) {
      sql.append(sqlParams.getCustomSQL());
    } else {
      sql.append("SELECT");
      if (DatabaseUtils.isNotEmpty(sqlParams.getSelectColumns())) {
        sql.append(' ').append(sqlParams.getSelectColumns()).append(' ');
      } else if (DatabaseUtils.isNotEmpty(sqlParams.getExcludedColumns())) {
        sql.append(' ').append(
            DatabaseUtils
                .buildColumns(sqlParams.getExcludedColumns(), sqlParams.getModelClass()))
            .append(' ');
      } else {
        sql.append(" * ");
      }
      sql.append("FROM ").append(sqlParams.getTableName());
      if (sqlParams.getConditionSQL().length() > 0) {
        sql.append(" WHERE ").append(sqlParams.getConditionSQL().substring(5));
      }
    }

    if (DatabaseUtils.isNotEmpty(sqlParams.getOrderBy())) {
      sql.append(" ORDER BY").append(sqlParams.getOrderBy());
    }
    if (sqlParams.isSQLLimit()) {
      sql.append(" LIMIT ?");
    }
    return sql.toString();
  }

  default String count(SQLParams sqlParams) {
    StringBuilder sql = new StringBuilder();
    sql.append("SELECT COUNT(*) FROM ").append(sqlParams.getTableName());
    if (sqlParams.getConditionSQL().length() > 0) {
      sql.append(" WHERE ").append(sqlParams.getConditionSQL().substring(5));
    }
    return sql.toString();
  }

  default String insert(SQLParams sqlParams) {
    StringBuilder sql = new StringBuilder("INSERT INTO ");
    sql.append(sqlParams.getTableName());

    StringBuilder columnNames = new StringBuilder();
    StringBuilder placeholder = new StringBuilder();

    List<Field> fields = DatabaseCache.computeModelFields(sqlParams.getModelClass());

    for (int i = 0; i < fields.size(); i++) {
      if (null != sqlParams.getColumnValues().get(i)) {
        Field field = fields.get(i);
        columnNames.append(",").append(" ").append(DatabaseCache.getColumnName(field));
        placeholder.append(", ?");
      }
    }

    ifThen(columnNames.length() > 0 && placeholder.length() > 0,
        () -> sql.append("(").append(columnNames.substring(2)).append(")").append(" VALUES (")
            .append(placeholder.substring(2)).append(")"));

    return sql.toString();
  }

  default String insertOnDuplicate(SQLParams sqlParams) {
    StringBuilder sql = new StringBuilder();
    sql.append("INSERT INTO ").append(sqlParams.getTableName());

    StringBuilder columnNames = new StringBuilder();
    StringBuilder placeholder = new StringBuilder();

    StringBuilder duplicateColumnNames = new StringBuilder();

    List<Field> fields = DatabaseCache.computeModelFields(sqlParams.getModelClass());

    for (int i = 0; i < fields.size(); i++) {
      if (null != sqlParams.getColumnValues().get(i)) {
        Field field = fields.get(i);
        columnNames.append(",").append(" ").append(DatabaseCache.getColumnName(field));
        placeholder.append(", ?");
        if (DatabaseCache.updateOnDuplicate(field)) {
          duplicateColumnNames.append(",").append(" ")
              .append(DatabaseCache.getColumnName(field)).append(" = ").append(" ? ");
        }
      }
    }

    ifThen(columnNames.length() > 0 && placeholder.length() > 0,
        () -> sql.append("(").append(columnNames.substring(2)).append(")").append(" VALUES (")
            .append(placeholder.substring(2)).append(")"));

    ifThen(duplicateColumnNames.length() > 0,
        () -> sql.append(" ON DUPLICATE KEY UPDATE ").append(duplicateColumnNames.substring(2)));

    return sql.toString();
  }

  default String update(SQLParams sqlParams) {
    StringBuilder sql = new StringBuilder();
    sql.append("UPDATE ").append(sqlParams.getTableName()).append(" SET ");

    StringBuilder setSQL = new StringBuilder();

    if (null != sqlParams.getUpdateColumns() && !sqlParams.getUpdateColumns().isEmpty()) {
      sqlParams.getUpdateColumns().forEach((key, value) -> setSQL.append(key).append(" = ?, "));
    } else {
      if (null != sqlParams.getModel()) {
        for (Field field : DatabaseCache.computeModelFields(sqlParams.getModelClass())) {
          try {
            Object value = DatabaseUtils
                .invokeMethod(sqlParams.getModel(), getGetterName(field.getName()),
                    DatabaseUtils.EMPTY_ARG);
            if (null == value) {
              continue;
            }
            setSQL.append(DatabaseCache.getColumnName(field)).append(" = ?, ");
          } catch (IllegalArgumentException e) {
            throw new DatabaseWrapperException("illegal argument or Access:", e);
          }
        }
      }
    }
    sql.append(setSQL.substring(0, setSQL.length() - 2));
    if (sqlParams.getConditionSQL().length() > 0) {
      sql.append(" WHERE ").append(sqlParams.getConditionSQL().substring(5));
    }
    return sql.toString();
  }

  default String delete(SQLParams sqlParams) {
    StringBuilder sql = new StringBuilder();
    sql.append("DELETE FROM ").append(sqlParams.getTableName());

    if (sqlParams.getConditionSQL().length() > 0) {
      sql.append(" WHERE ").append(sqlParams.getConditionSQL().substring(5));
    } else {
      if (null != sqlParams.getModel()) {
        StringBuilder columnNames = new StringBuilder();
        for (Field field : DatabaseCache.computeModelFields(sqlParams.getModelClass())) {
          try {
            Object value = DatabaseUtils
                .invokeMethod(sqlParams.getModel(), getGetterName(field.getName()),
                    DatabaseUtils.EMPTY_ARG);
            if (null == value) {
              continue;
            }
            columnNames.append(DatabaseCache.getColumnName(field)).append(" = ? and ");
          } catch (IllegalArgumentException e) {
            throw new DatabaseWrapperException("illegal argument or Access:", e);
          }
        }
        if (columnNames.length() > 0) {
          sql.append(" WHERE ").append(columnNames.substring(0, columnNames.length() - 5));
        }
      }
    }
    return sql.toString();
  }

  String paginate(SQLParams sqlParams);

}
