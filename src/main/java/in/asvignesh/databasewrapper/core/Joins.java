package in.asvignesh.databasewrapper.core;


import in.asvignesh.databasewrapper.DataModel;

public class Joins {

  public static JoinParam with(Class<? extends DataModel> joinModel) {
    return new JoinParam(joinModel);
  }

}
