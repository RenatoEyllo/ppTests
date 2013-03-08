package com.eyllo.test.performance.operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class performing an operation
 * @author rmarroquin
 */
public abstract class AbstractOperation extends Thread{

  /**
   * Object to help us log operation behavior
   */
  public static final Logger LOG = LoggerFactory.getLogger(AbstractOperation.class);
  
  /**
   * Object containing the measure of the operation
   */
  private float measurement;
  
  /**
   * Default name operation
   */
  private static String DEFAULT_OP_NAME = "unnameOp";

  /**
   * Waiting time before processing starts
   */
  public static int DEFAULT_WAIT_TIME = 1000;

  /**
   * Default constructor
   * @param pOpName
   */
  public AbstractOperation(String pOpName) {
    if (pOpName.equals(""))
      this.setName(DEFAULT_OP_NAME);
    else
      this.setName(pOpName);
  }

  /**
   * Method that runs the operation
   */
  @Override
  public void run() {
    getLogger().debug("Processing - START "+Thread.currentThread().getName());
    try {
      Thread.sleep(DEFAULT_WAIT_TIME);
      this.setMeasurement(doProcessing());
    } catch (InterruptedException e) {
      getLogger().error("Error while running "+Thread.currentThread().getName());
      e.printStackTrace();
    }
    getLogger().debug("Processing - END "+Thread.currentThread().getName());
  }

  /**
   * Method that should be overridden by all other operations
   * @return
   * @throws InterruptedException
   */
  public abstract float doProcessing() throws InterruptedException;

  /**
   * Getter for the measurement of the operation being tested
   * @return
   */
  public float getMeasurement() {
    return measurement;
  }

  /**
   * Setter for the measurement of the operation being tested
   * @param measurement
   */
  public void setMeasurement(float measurement) {
    this.measurement = measurement;
  }

  /**
   * Gets operation class logger object
   * @return
   */
  public static Logger getLogger(){
    return LOG;
  }
}
