package in.asvignesh.databasewrapper.dialect;


import in.asvignesh.databasewrapper.core.SQLParams;
import in.asvignesh.databasewrapper.page.PageRow;

public class MySQLDialect implements Dialect {

  @Override
  public String paginate(SQLParams sqlParams) {
    PageRow pageRow = sqlParams.getPageRow();
    int limit = pageRow.getPageSize();
    int offset = limit * (pageRow.getPageNum() - 1);
    String limitSQL = " LIMIT " + offset + "," + limit;

    StringBuilder sql = new StringBuilder();
    sql.append(select(sqlParams)).append(limitSQL);
    return sql.toString();
  }


}
