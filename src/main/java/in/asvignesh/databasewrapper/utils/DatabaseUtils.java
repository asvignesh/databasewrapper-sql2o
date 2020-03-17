package in.asvignesh.databasewrapper.utils;


import static in.asvignesh.databasewrapper.core.DatabaseCache.METHOD_ACCESS_MAP;
import static in.asvignesh.databasewrapper.core.DatabaseCache.computeModelFields;
import static in.asvignesh.databasewrapper.core.DatabaseCache.getColumnName;
import static in.asvignesh.databasewrapper.core.DatabaseCache.getGetterName;
import static in.asvignesh.databasewrapper.core.DatabaseCache.getPKField;
import static in.asvignesh.databasewrapper.core.DatabaseCache.getSetterName;
import static in.asvignesh.databasewrapper.core.DatabaseCache.isIgnore;

import com.blade.reflectasm.MethodAccess;
import in.asvignesh.databasewrapper.DataModel;
import in.asvignesh.databasewrapper.core.DatabaseCache;
import in.asvignesh.databasewrapper.exception.DatabaseWrapperException;
import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.sql2o.converters.Converter;

/**
 * Utility class for composing SQL statements
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DatabaseUtils {

  public static final Object[] EMPTY_ARG = new Object[]{};
  public static final Object[] NULL_ARG = new Object[]{null};

  public static boolean isNotEmpty(String value) {
    return null != value && !value.isEmpty();
  }

  public static boolean isNotEmpty(Collection<?> collection) {
    return null != collection && !collection.isEmpty();
  }

  public static boolean isEmpty(String value) {
    return null == value || value.isEmpty();
  }

  public static String toCamelName(String value) {
    String[] partOfNames = value.split("_");

    StringBuilder sb = new StringBuilder(partOfNames[0]);
    for (int i = 1; i < partOfNames.length; i++) {
      sb.append(partOfNames[i].substring(0, 1).toUpperCase());
      sb.append(partOfNames[i].substring(1));
    }
    return sb.toString();
  }

  public static String toUnderline(String value) {
    StringBuilder result = new StringBuilder();
    if (value != null && value.length() > 0) {
      result.append(value.substring(0, 1).toLowerCase());
      for (int i = 1; i < value.length(); i++) {
        String s = value.substring(i, i + 1);
        if (s.equals(s.toUpperCase())) {
          result.append("_");
          result.append(s.toLowerCase());
        } else {
          result.append(s);
        }
      }
    }
    return result.toString();
  }

  public static <T extends DataModel> List<Object> toColumnValues(T model,
      boolean allowNull) {
    List<Object> columnValueList = new ArrayList<>();
    for (Field field : computeModelFields(model.getClass())) {
      try {
        Object value = invokeMethod(model, getGetterName(field.getName()), EMPTY_ARG);
        if (null == value) {
          if (allowNull) {
            columnValueList.add(null);
          }
          continue;
        }
        columnValueList.add(value);
      } catch (IllegalArgumentException e) {
        throw new DatabaseWrapperException("illegal argument or Access:", e);
      }
    }
    return columnValueList;
  }

  public static <T extends DataModel> List<Object> toColumnValuesDuplicate(T model,
      boolean allowNull) {
    List<Object> columnValueList = new ArrayList<>();
    for (Field field : computeModelFields(model.getClass())) {
      try {
        if (DatabaseCache.updateOnDuplicate(field)) {
          Object value = invokeMethod(model, getGetterName(field.getName()), EMPTY_ARG);
          if (null == value) {
            if (allowNull) {
              columnValueList.add(null);
            }
            continue;
          }
          columnValueList.add(value);
        }
      } catch (IllegalArgumentException e) {
        throw new DatabaseWrapperException("illegal argument or Access:", e);
      }
    }
    return columnValueList;
  }

  public static <T extends DataModel> String buildColumns(List<String> excludedColumns,
      Class<T> modelClass) {
    StringBuilder sql = new StringBuilder();
    for (Field field : computeModelFields(modelClass)) {
      String columnName = getColumnName(field);
      if (!isIgnore(field) && !excludedColumns.contains(columnName)) {
        sql.append(columnName).append(',');
      }
    }
    if (sql.length() > 0) {
      return sql.substring(0, sql.length() - 1);
    }
    return "*";
  }

  public static Object invokeMethod(Object target, String methodName, Object[] args) {
    MethodAccess methodAccess = METHOD_ACCESS_MAP.computeIfAbsent(target.getClass(), type -> {
      List<Method> methods = Arrays.asList(type.getDeclaredMethods());
      return MethodAccess.get(type, methods);
    });
    return methodAccess.invokeWithCache(target, methodName, args);
  }

  public static String getLambdaColumnName(Serializable lambda) {
    SerializedLambda serializedLambda = computeSerializedLambda(lambda);
    return DatabaseCache.getLambdaColumnName(serializedLambda);
  }

  public static String getLambdaFieldName(Serializable lambda) {
    SerializedLambda serializedLambda = computeSerializedLambda(lambda);
    return DatabaseCache.getLambdaFieldName(serializedLambda);
  }

  private static SerializedLambda computeSerializedLambda(Serializable lambda) {
    for (Class<?> cl = lambda.getClass(); cl != null; cl = cl.getSuperclass()) {
      try {
        Method m = cl.getDeclaredMethod("writeReplace");
        m.setAccessible(true);
        Object replacement = m.invoke(lambda);
        if (!(replacement instanceof SerializedLambda)) {
          break; // custom interface implementation
        }
        return (SerializedLambda) replacement;
      } catch (Exception e) {
        throw new DatabaseWrapperException("get lambda column name fail", e);
      }
    }
    return null;
  }

  public static String methodToFieldName(String methodName) {
    return capitalize(methodName.replace("get", ""));
  }

  public static String capitalize(String input) {
    return input.substring(0, 1).toLowerCase() + input.substring(1);
  }

  public static <S extends DataModel> Object getAndRemovePrimaryKey(S model) {
    String fieldName = getPKField(model.getClass());
    Object value = invokeMethod(model, getGetterName(fieldName), EMPTY_ARG);
    if (null != value) {
      invokeMethod(model, getSetterName(fieldName), NULL_ARG);
    }
    return value;
  }

  public static <T> T[] toArray(List<T> list) {
    T[] toR = (T[]) Array.newInstance(list.get(0).getClass(), list.size());
    for (int i = 0; i < list.size(); i++) {
      toR[i] = list.get(i);
    }
    return toR;
  }

  public static boolean isBasicType(Class<?> type) {
    return type.equals(char.class) ||
        type.equals(Character.class) ||
        type.equals(boolean.class) ||
        type.equals(Boolean.class) ||
        type.equals(byte.class) ||
        type.equals(Byte.class) ||
        type.equals(short.class) ||
        type.equals(Short.class) ||
        type.equals(int.class) ||
        type.equals(Integer.class) ||
        type.equals(long.class) ||
        type.equals(Long.class) ||
        type.equals(BigDecimal.class) ||
        type.equals(BigInteger.class) ||
        type.equals(Date.class) ||
        type.equals(String.class) ||
        type.equals(double.class) ||
        type.equals(Double.class) ||
        type.equals(float.class) ||
        type.equals(Float.class);
  }

  public static Class getConverterType(Converter<?> converter) {
    Type[] types = converter.getClass().getGenericInterfaces();
    Type[] params = ((ParameterizedType) types[0]).getActualTypeArguments();
    return (Class) params[0];
  }

}
