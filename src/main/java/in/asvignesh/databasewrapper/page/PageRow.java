package in.asvignesh.databasewrapper.page;

import lombok.Data;

@Data
public class PageRow {

  private int pageNum;
  private int pageSize;

  public PageRow(int pageNum, int pageSize) {
    this.pageNum = pageNum;
    this.pageSize = pageSize;
  }

}