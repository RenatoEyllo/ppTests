package com.eyllo.test.performance;

import com.eyllo.test.performance.operation.AbstractOperation;

/**
 * Class that runs all operation threads
 * @author rmarroquin
 */
public class PerformanceTest {

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

  public final boolean startThreads(Class<?> pOperation){
    System.out.println("Starting Runnable threads");
    boolean result = false;
    try{
      for(int iCnt = 0; iCnt < this.numThreads; iCnt++){
        Thread tmpThread = (Thread) pOperation.newInstance();
        tmpThread.setName("t"+String.valueOf(iCnt) + "-" + tmpThread.getName());
        System.out.println("Starting Thread " + tmpThread.getName());
        this.runningThreads[iCnt] = tmpThread;
        this.runningThreads[iCnt].start();
        System.out.println(String.valueOf(iCnt) + " threads started.");
      }
      result = true;
    }catch(Exception e){
      System.out.println("Error while starting threads.");
      e.printStackTrace();
    }
    return result;
  }

  public final synchronized boolean waitForCompleting(){
    System.out.println("Waiting for Runnable threads to finish.");
    boolean result = false;
    try {
      for (int iCnt = 0; iCnt < this.numThreads; iCnt++) {
          this.runningThreads[iCnt].join();
          this.execTime[iCnt] = ((AbstractOperation)this.runningThreads[iCnt]).getMeasurement();
      }
      result = true;
    } catch (InterruptedException e) {
      System.out.println("Error while waiting for threads.");
      e.printStackTrace();
    }
    return result;
  }

  @SuppressWarnings("deprecation")
  public final boolean stopThreads(){
    boolean result = false;
    try{
      for(int iCnt = 0; iCnt < this.numThreads; iCnt++)
        this.runningThreads[iCnt].stop();
      result = true;
    }catch(Exception e){
      result = false;
    }
    return result;
  }

  public void setMeasureCount(int pMeasureCount){
    this.measureCount = pMeasureCount;
  }

  public double getAvgExecTime(){
    double avg = 0;
    for(int iCnt = 0; iCnt < this.measureCount; iCnt++)
      avg += getExecTime()[iCnt];
    this.avgExecTime = avg / this.measureCount;
    return this.avgExecTime;
  }

  public double[] getExecTime() {
    return execTime;
  }

  public void setExecTime(double execTime[]) {
    this.execTime = execTime;
  }

  public void setRunningThreads(Thread pRunningThreads[]){
    this.runningThreads = pRunningThreads;
  }

  public int getNumThreads() {
    return numThreads;
  }

  public void setNumThreads(int numThreads) {
    this.numThreads = numThreads;
  }

}
