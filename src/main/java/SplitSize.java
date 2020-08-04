public class SplitSize {
  private long splitSize;
  private static SplitSize instance;

  private SplitSize(){}

  public static SplitSize getInstance(){
    if(instance == null)
      instance = new SplitSize();
    return instance;
  }

  public void setSplitSize (String splitSize){
    // parse to MB
    this.splitSize = Long.parseLong(splitSize) * 1024 * 1024;
  }

  public long getSplitSize(){
    return splitSize;
  }
}
