package com.eyllo.paprika.test.stress.operation;

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
   * Number of write operations to be performed
   */
  private int operations;

  /**
   * Waiting time between requests
   */
  private int waitTime;

  /**
   * Default name operation
   */
  private static String DEFAULT_OP_NAME = "unnameOp";

  /**
   * Waiting time before processing starts
   */
  private static int DEFAULT_WAIT_TIME = 1000;

  /**
   * Default number of operations to be performed
   */
  private static int DEFAULT_OP_NUMBER = 10;

  /**
   * Default constructor
   * @param pOpName
   */
  public AbstractOperation() {
    this.setName(DEFAULT_OP_NAME);
    this.setWaitTime(DEFAULT_WAIT_TIME);
    this.setOperations(DEFAULT_OP_NUMBER);
  }

  /**
   * Constructor using all necessary parameters.
   * If there are parameters missing, then the default ones will be used.
   * @param pOpName
   * @param pWaitTime
   * @param pOpNumber
   */
  public AbstractOperation(String pOpName, int pWaitTime, int pOpNumber) {
    if (pOpName.equals(""))
      this.setName(DEFAULT_OP_NAME);
    else
      this.setName(pOpName);
    if (pWaitTime <= 0){
      getLogger().info("Using DEFAULT_WAIT_TIME");
      this.setWaitTime(DEFAULT_WAIT_TIME);
    }
    else 
      this.setWaitTime(pWaitTime);
    if (pOpNumber <= 0){
      getLogger().info("Using DEFAULT_OP_NAME");
      this.setOperations(DEFAULT_OP_NUMBER);
    }
    else
      this.setOperations(pOpNumber);
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

  public static int getDefaultWaitTime() {
    return DEFAULT_WAIT_TIME;
  }

  public static void setDefaultWaitTime(int pDefaultWaitTime) {
    DEFAULT_WAIT_TIME = pDefaultWaitTime;
  }
  
  /**
   * Getter for readOperation attribute
   * @return
   */
  public int getOperations() {
    return operations;
  }

  /**
   * Setter for readOperation attribute
   * @param writeOperations
   */
  public void setOperations(int pOperations) {
    this.operations = pOperations;
  }

  public static int getDefaultOpNumber() {
    return DEFAULT_OP_NUMBER;
  }

  public static void setDefaultOpNumber(int pDefaultOpNumber) {
    DEFAULT_OP_NUMBER = pDefaultOpNumber;
  }

  public int getWaitTime() {
    return waitTime;
  }

  public void setWaitTime(int waitTime) {
    this.waitTime = waitTime;
  }
}
