package com.eyllo.test.performance.operation;

/**
 * Abstract class performing an operation
 * @author rmarroquin
 */
public abstract class AbstractOperation extends Thread{

  private float measurement;
  
  private static String DEFAULT_OP_NAME = "unnameOp";

  /**
   * Waiting time before processing starts
   */
  public static int DEFAULT_WAIT_TIME = 1000;

  public AbstractOperation(String pOpName) {
    if (pOpName.equals(""))
      this.setName(DEFAULT_OP_NAME);
    else
      this.setName(pOpName);
  }

  @Override
  public void run() {
      System.out.println("Processing - START "+Thread.currentThread().getName());
      try {
          Thread.sleep(DEFAULT_WAIT_TIME);
          this.setMeasurement(doProcessing());
      } catch (InterruptedException e) {
          e.printStackTrace();
      }
      System.out.println("Processing - END "+Thread.currentThread().getName());
  }

  public abstract float doProcessing() throws InterruptedException;

  public float getMeasurement() {
    return measurement;
  }

  public void setMeasurement(float measurement) {
    this.measurement = measurement;
  }
}
