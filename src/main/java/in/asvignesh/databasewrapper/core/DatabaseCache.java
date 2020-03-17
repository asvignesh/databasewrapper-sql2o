package in.asvignesh.databasewrapper.core;

import static in.asvignesh.databasewrapper.utils.DatabaseUtils.methodToFieldName;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import com.blade.reflectasm.MethodAccess;
import in.asvignesh.databasewrapper.DatabaseWrapper;
import in.asvignesh.databasewrapper.annotation.Column;
import in.asvignesh.databasewrapper.annotation.Ignore;
import in.asvignesh.databasewrapper.annotation.Table;
import in.asvignesh.databasewrapper.exception.DatabaseWrapperException;
import in.asvignesh.databasewrapper.utils.DatabaseUtils;
import in.asvignesh.databasewrapper.utils.English;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;

@UtilityClass
public class DatabaseCache {

  public static final Map<Class, MethodAccess> METHOD_ACCESS_MAP = new ConcurrentHashMap<>();

  private static final Map<Class<?>, String> CACHE_TABLE_NAME = new ConcurrentHashMap<>(8);
  private static final Map<Class<?>, String> CACHE_PK_COLUMN_NAME = new ConcurrentHashMap<>(8);
  private static final Map<Class<?>, String> CACHE_PK_FIELD_NAME = new ConcurrentHashMap<>(8);
  private static final Map<Class<?>, Map<String, String>> MODEL_COLUMN_MAPPINGS = new ConcurrentHashMap<>(
      8);
  private static final Map<SerializedLambda, String> CACHE_LAMBDA_NAME = new ConcurrentHashMap<>(8);
  private static final Map<SerializedLambda, String> CACHE_FIELD_NAME = new ConcurrentHashMap<>(8);

  private static final Map<String, String> GETTER_METHOD_NAME = new ConcurrentHashMap<>();
  private static final Map<String, String> SETTER_METHOD_NAME = new ConcurrentHashMap<>();
  private static final Map<String, String> FIELD_COLUMN_NAME = new ConcurrentHashMap<>();
  private static final Map<String, Boolean> FIELD_COLUMN_ONDUPLICATE = new ConcurrentHashMap<>();
  private static final Map<Class, List<Field>> MODEL_AVAILABLE_FIELDS = new ConcurrentHashMap<>();

  public static Map<String, String> computeModelColumnMappings(Class<?> modelType) {
    return MODEL_COLUMN_MAPPINGS.computeIfAbsent(modelType, model -> {
      List<Field> fields = computeModelFields(model);
      return fields.stream()
          .collect(toMap(DatabaseCache::getColumnName, Field::getName));
    });
  }

  public static List<Field> computeModelFields(Class clazz) {
    return MODEL_AVAILABLE_FIELDS.computeIfAbsent(clazz, model ->
        Stream.of(model.getDeclaredFields())
            .filter(field -> !isIgnore(field))
            .collect(toList()));
  }

  public static String getTableName(String className, String prefix) {
    boolean hasPrefix = prefix != null && prefix.trim().length() > 0;
    return hasPrefix ? English.plural(prefix + "_" + DatabaseUtils.toUnderline(className), 2)
        : English.plural(DatabaseUtils.toUnderline(className), 2);
  }

  public static String getColumnName(Field field) {
    String fieldName = field.getName();
    String key = field.getDeclaringClass().getSimpleName() + "_" + fieldName;

    return FIELD_COLUMN_NAME.computeIfAbsent(key, f -> {
      Column column = field.getAnnotation(Column.class);
      if (null != column) {
        return column.name();
      }
      return DatabaseUtils.toUnderline(fieldName);
    });
  }

  public static Boolean updateOnDuplicate(Field field) {
    String fieldName = field.getName();
    String key = field.getDeclaringClass().getSimpleName() + "_" + fieldName;
    return FIELD_COLUMN_ONDUPLICATE.computeIfAbsent(key, f -> {
      Column column = field.getAnnotation(Column.class);
      if (null != column) {
        return column.updateOnDuplicate();
      }
      return false;
    });
  }

  public static String getGetterName(String fieldName) {
    return GETTER_METHOD_NAME.computeIfAbsent(fieldName,
        name -> "get" + Character.toUpperCase(name.charAt(0)) + name.substring(1));
  }

  public static String getSetterName(String fieldName) {
    return SETTER_METHOD_NAME.computeIfAbsent(fieldName,
        name -> "set" + Character.toUpperCase(name.charAt(0)) + name.substring(1));
  }

  public static String getTableName(Class<?> modelClass) {
    return CACHE_TABLE_NAME.computeIfAbsent(modelClass, type -> {
      Table table = type.getAnnotation(Table.class);
      if (null != table && DatabaseUtils.isNotEmpty(table.name())) {
        return table.name();
      }
      return getTableName(type.getSimpleName(), DatabaseWrapper.of().tablePrefix());
    });
  }

  public static String getPKColumn(Class<?> modelClass) {
    String pkColumn = CACHE_PK_COLUMN_NAME.get(modelClass);
    if (null != pkColumn) {
      return pkColumn;
    }
    Table table = modelClass.getAnnotation(Table.class);
    pkColumn = null != table ? table.pk() : "id";
    CACHE_PK_COLUMN_NAME.put(modelClass, pkColumn);
    return pkColumn;
  }

  public static String getPKField(Class<?> modelClass) {
    String pkField = CACHE_PK_FIELD_NAME.get(modelClass);
    if (null != pkField) {
      return pkField;
    }
    String pkColumn = DatabaseCache.getPKColumn(modelClass);
    pkField = DatabaseUtils.toCamelName(pkColumn);
    CACHE_PK_FIELD_NAME.put(modelClass, pkField);
    return pkField;
  }

  public static String getLambdaColumnName(SerializedLambda serializedLambda) {
    return CACHE_LAMBDA_NAME.computeIfAbsent(serializedLambda, lambda -> {
      String className = serializedLambda.getImplClass().replace("/", ".");
      String methodName = serializedLambda.getImplMethodName();
      String fieldName = methodToFieldName(methodName);
      try {
        Field field = Class.forName(className).getDeclaredField(fieldName);
        return getColumnName(field);
      } catch (NoSuchFieldException | ClassNotFoundException e) {
        throw new DatabaseWrapperException(e);
      }
    });
  }

  public static String getLambdaFieldName(SerializedLambda serializedLambda) {
    String name = CACHE_FIELD_NAME.get(serializedLambda);
    if (null != name) {
      return name;
    }
    String methodName = serializedLambda.getImplMethodName();
    String fieldName = methodToFieldName(methodName);
    CACHE_FIELD_NAME.put(serializedLambda, fieldName);
    return fieldName;
  }

  public static boolean isIgnore(Field field) {
    if ("serialVersionUID".equals(field.getName())) {
      return true;
    }
    return null != field.getAnnotation(Ignore.class);
  }

}
