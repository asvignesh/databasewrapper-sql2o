package in.asvignesh.databasewrapper.core.dml;

import in.asvignesh.databasewrapper.DataModel;
import in.asvignesh.databasewrapper.core.Query;
import in.asvignesh.databasewrapper.enums.DMLType;

public class Delete {

  public <T extends DataModel> Query<T> from(Class<T> modelClass) {
    return new Query<T>(DMLType.DELETE).parse(modelClass);
  }
}
