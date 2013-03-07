package com.eyllo.test.performance;

import com.eyllo.test.performance.operation.PutOperation;

/**
 * Class in charge of running the specific operation test 
 * @author renatomarroquin
 *
 */
public class TestRunner {

  /**
   * @param args
   */
  public static void main(String[] args) {
    PerformanceTest perfTest = new PerformanceTest();
    perfTest.startThreads(PutOperation.class);
    perfTest.waitForCompleting();
    double execSecsTime = perfTest.getAvgExecTime() / 1000000000.0;
    System.out.format("%10.3f %s", execSecsTime, " seconds.");
  }

}
