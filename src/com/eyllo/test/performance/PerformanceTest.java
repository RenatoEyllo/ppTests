package com.eyllo.test.performance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.eyllo.test.performance.operation.AbstractOperation;

/**
 * Class that runs all operation threads
 * @author rmarroquin
 */
public class PerformanceTest {

  /**
   * Object to help us log performance test behavior
   */
  public static final Logger LOG = LoggerFactory.getLogger(AbstractOperation.class);
  
  /**
   * Default number of measurements stored in memory
   */
  private static int DEFAULT_MEASURE_COUNTS = 10;

  /**
   * Default number of threads to be run executing an operation
   */
  private static int DEFAULT_NUM_THREADS = 10;

  /**
   * Execution time from each operation
   */
  private double execTime[];

  /**
   * Average execution time from evaluating a single operation
   */
  private double avgExecTime;

  /**
   * Number of measurements that will be taken
   */
  private int measureCount;

  /**
   * Number of threads that will run the evaluate operation
   */
  private int numThreads;

  /**
   * Threads that will be run
   */
  private Thread runningThreads[];

  /**
   * Default constructor
   */
  public PerformanceTest() {
    this.setMeasureCount(DEFAULT_MEASURE_COUNTS);
    this.setNumThreads(DEFAULT_NUM_THREADS);
    this.setExecTime(new double[this.measureCount]);
    this.setRunningThreads(new Thread[this.numThreads]);
  }

  /**
   * Method that starts all threads
   * @param pOperation
   * @return
   */
  public final boolean startThreads(Class<?> pOperation){
    getLogger().info("Starting Runnable threads");
    boolean result = false;
    try{
      for(int iCnt = 0; iCnt < this.numThreads; iCnt++){
        Thread tmpThread = (Thread) pOperation.newInstance();
        tmpThread.setName("t"+String.valueOf(iCnt) + "-" + tmpThread.getName());
        getLogger().debug("Starting Thread " + tmpThread.getName());
        this.runningThreads[iCnt] = tmpThread;
        this.runningThreads[iCnt].start();
        System.out.println(String.valueOf(iCnt) + " threads started.");
      }
      result = true;
    }catch(Exception e){
      getLogger().error("Error while starting threads.");
      e.printStackTrace();
    }
    return result;
  }

  /**
   * Method that waits until all threads are completed
   * @return
   */
  public final synchronized boolean waitForCompleting(){
    getLogger().info("Waiting for Runnable threads to finish.");
    boolean result = false;
    try {
      for (int iCnt = 0; iCnt < this.numThreads; iCnt++) {
          this.runningThreads[iCnt].join();
          this.execTime[iCnt] = ((AbstractOperation)this.runningThreads[iCnt]).getMeasurement();
      }
      result = true;
    } catch (InterruptedException e) {
      getLogger().error("Error while waiting for threads.");
      e.printStackTrace();
    }
    return result;
  }

  /**
   * Method that stops all threads no matter what
   * @return
   */
  @SuppressWarnings("deprecation")
  public final boolean stopThreads(){
    getLogger().info("Stopping Runnable threads to finish.");
    boolean result = false;
    try{
      for(int iCnt = 0; iCnt < this.numThreads; iCnt++)
        this.runningThreads[iCnt].stop();
      result = true;
    }catch(Exception e){
      getLogger().error("Error while stopping threads.");
      result = false;
    }
    return result;
  }

  /**
   * Setter for the measure count
   * @param pMeasureCount
   */
  public void setMeasureCount(int pMeasureCount){
    this.measureCount = pMeasureCount;
  }

  /**
   * Obtains the average time of executing an operation
   * @return
   */
  public double getAvgExecTime(){
    double avg = 0;
    for(int iCnt = 0; iCnt < this.measureCount; iCnt++)
      avg += getExecTime()[iCnt];
    this.avgExecTime = avg / this.measureCount;
    return this.avgExecTime;
  }

  /**
   * Getter for execution time measures
   * @return
   */
  public double[] getExecTime() {
    return execTime;
  }

  /**
   * Setter for execution time measures
   * @param execTime
   */
  public void setExecTime(double execTime[]) {
    this.execTime = execTime;
  }

  /**
   * Setter for runningThreads
   * @param pRunningThreads
   */
  public void setRunningThreads(Thread pRunningThreads[]){
    this.runningThreads = pRunningThreads;
  }

  /**
   * Gets the number of threads being run
   * @return
   */
  public int getNumThreads() {
    return numThreads;
  }

  /**
   * Sets the number of threads that will be run
   * @param numThreads
   */
  public void setNumThreads(int numThreads) {
    this.numThreads = numThreads;
  }

  /**
   * Gets operation class logger object
   * @return
   */
  public static Logger getLogger(){
    return LOG;
  }
}
