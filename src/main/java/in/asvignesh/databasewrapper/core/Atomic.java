package in.asvignesh.databasewrapper.core;

import java.util.function.Consumer;
import java.util.function.Function;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class Atomic {

  private Exception e;
  private boolean isRollback;

  public Atomic(Exception e) {
    this.e = e;
  }

  public static Atomic ok() {
    return new Atomic();
  }

  public static Atomic error(Exception e) {
    return new Atomic(e);
  }

  public Atomic rollback(boolean isRollback) {
    this.isRollback = isRollback;
    return this;
  }

  public boolean isRollback() {
    return isRollback;
  }

  public Atomic catchException(Consumer<Exception> consumer) {
    if (null != e) {
      consumer.accept(e);
    }
    return this;
  }

  public <R> R catchAndReturn(Function<Exception, R> function) {
    if (null != e) {
      return function.apply(e);
    }
    return null;
  }

}
