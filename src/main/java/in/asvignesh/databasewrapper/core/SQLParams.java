package in.asvignesh.databasewrapper.core;

import in.asvignesh.databasewrapper.DataModel;
import in.asvignesh.databasewrapper.page.PageRow;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SQLParams {

  private Class<? extends DataModel> modelClass;
  private Object model;
  private String selectColumns;
  private String tableName;
  private String pkName;
  private StringBuilder conditionSQL;
  private List<Object> columnValues;
  private Map<String, Object> updateColumns;
  private List<String> excludedColumns;
  private PageRow pageRow;
  private String orderBy;
  private boolean isSQLLimit;

  private String customSQL;

}
