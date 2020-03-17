package in.asvignesh.databasewrapper.core.dml;

import in.asvignesh.databasewrapper.DataModel;
import in.asvignesh.databasewrapper.core.Query;
import in.asvignesh.databasewrapper.core.ResultList;
import java.util.Map;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class Select {

  private String columns;

  public Select(String columns) {
    this.columns = columns;
  }

  public <T extends DataModel> Query<T> from(Class<T> modelClass) {
    return new Query<>(modelClass).select(this.columns);
  }

  public <T> ResultList<T> bySQL(Class<T> type, String sql, Object... params) {
    return new ResultList<>(type, sql, params);
  }

  public <T extends Map<String, Object>> ResultList<T> bySQL(String sql, Object... params) {
    return new ResultList<>(null, sql, params);
  }

}
