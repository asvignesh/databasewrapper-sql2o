package in.asvignesh.databasewrapper.core;

import java.math.BigInteger;

public class ResultKey {

  private Object key;

  public ResultKey(Object key) {
    this.key = key;
  }

  public Integer asInt() {
    if (key instanceof Long) {
      return asLong().intValue();
    }
    if (key instanceof BigInteger) {
      return asBigInteger().intValue();
    }
    return (Integer) key;
  }


  public Long asLong() {
    return (Long) key;
  }

  public BigInteger asBigInteger() {
    return (BigInteger) key;
  }

  public String asString() {
    return key.toString();
  }

}
