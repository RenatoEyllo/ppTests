package com.eyllo.test.performance.operation;

/**
 * Class in charged of the put operation for Paprika Application
 * @author renatomarroquin
 *
 */
public class PutOperation extends AbstractOperation{

  /**
   * Number of write operations to be performed
   */
  private int writeOperations = 10000;

  /**
   * Operation name
   */
  private static String operationName = "PutOperation";

  /**
   * Default constructor for the PutOperation
   */
  public PutOperation(){
    super(operationName);
  }

  @Override
  public float doProcessing() throws InterruptedException {
    long startTime = System.nanoTime();
    // TODO Connect to application and writing
    for (int iCnt = 0; iCnt < this.writeOperations; iCnt++);
    long endTime = System.nanoTime();
    
    return endTime - startTime;
  }

  public int getWriteOperations() {
    return writeOperations;
  }

  public void setWriteOperations(int writeOperations) {
    this.writeOperations = writeOperations;
  }

}
