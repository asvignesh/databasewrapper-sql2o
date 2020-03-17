package in.asvignesh.databasewrapper.core;


import in.asvignesh.databasewrapper.DataModel;
import in.asvignesh.databasewrapper.page.Page;
import in.asvignesh.databasewrapper.page.PageRow;
import java.util.List;
import java.util.Map;

public class ResultList<T> {

  private final Class<T> type;
  private final String sql;
  private final Object[] params;

  public ResultList(Class<T> type, String sql, Object[] params) {
    this.type = type;
    this.sql = sql;
    this.params = params;
  }

  public T one() {
    return new Query<>().useSQL().queryOne(type, sql, params);
  }

  public List<T> all() {
    return new Query<>().useSQL().queryList(type, sql, params);
  }

  public List<Map<String, Object>> maps() {
    return new Query<>().useSQL().queryListMap(sql, params);
  }

  public <S extends DataModel> Page<S> page(PageRow pageRow) {
    Class<S> modelType = (Class<S>) type;
    return new Query<>(modelType).useSQL().page(sql, params, pageRow);
  }

  public <S extends DataModel> Page<S> page(int page, int limit) {
    return this.page(new PageRow(page, limit));
  }

}
