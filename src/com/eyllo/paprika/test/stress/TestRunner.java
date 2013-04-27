package com.eyllo.paprika.test.stress;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.eyllo.paprika.test.exception.PaprikaStressException;
import com.eyllo.paprika.test.stress.operation.ReadOperation;
import com.eyllo.paprika.test.stress.operation.WriteOperation;

/**
 * Class in charge of running the specific operation test 
 * @author renatomarroquin
 *
 */
public class TestRunner {

  /**
   * Object to help us log operation behavior
   */
  private static final Logger LOG = LoggerFactory.getLogger(TestRunner.class);
  
  /**
   * Performance test to be executed.
   */
  private static PerformanceTest perfTest;
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      Options options = createOptions();

      if (args.length < 1){
        printUsage();
        throw new PaprikaStressException("Must provide at least three arguments.");
      }
    
      CommandLineParser parser = new PosixParser();
      CommandLine cmd = parser.parse(options, args);
      
      initialize( cmd.getOptionValue("op_name"), 
                  cmd.getOptionValue("thread_num", ""),
                  cmd.getOptionValue("op_number", ""),
                  cmd.getOptionValue("wait_time",""));
      
      execute();
      
    } catch (ParseException e) {
      e.printStackTrace();
      throw new PaprikaStressException("Error while parsing command options.");
    }
  }

  /**
   * Executes and prints average time of read request time
   */
  private static void execute(){
    //perfTest.startThreads(ReadOperation.class);
    getLog().info("Performing tests");
    perfTest.waitForCompleting();
    double execSecsTime = perfTest.getAvgExecTime() / 1000000000.0;
    getLog().info("Average execution time: " + String.valueOf(execSecsTime)
                  + " secs for " + perfTest.getOpNumber() + " operations.");
  }

  /**
   * Initializes the test to be executed.
   * @param pOpName
   * @param pThreadNumber
   * @param oOpNumber
   */
  private static void initialize(String pOpName, String pThreadNumber, String oOpNumber, String pWaitTime){
    getLog().info("Initializing tests for " + pOpName + " operation");
    perfTest = new PerformanceTest();
    if (pOpName.equals("read")){
      perfTest.setThreadsNum(Integer.parseInt(pThreadNumber));
      perfTest.setOpNumber(Integer.parseInt(oOpNumber));
      perfTest.setWaitTime(Integer.parseInt(pWaitTime));
      perfTest.startThreads(ReadOperation.class);
    }
    else if (pOpName.equals("write")){
      perfTest.setThreadsNum(Integer.parseInt(pThreadNumber));
      perfTest.setOpNumber(Integer.parseInt(oOpNumber));
      perfTest.setWaitTime(Integer.parseInt(pWaitTime));
      perfTest.startThreads(WriteOperation.class);
    }
    else
      getLog().error("Error processing " + pOpName + " operation");
  }

  /**
   * Creates the options gotten from the command line
   * @return
   */
  @SuppressWarnings("static-access")
  private static Options createOptions() {
        Options options = new Options();

        Option opName = OptionBuilder.withArgName("op_name").hasArg()
            .withDescription("Operation which will be tested.").create("op_name");
        options.addOption(opName);

        Option threadNumber = OptionBuilder.withArgName("thread_num").hasArg()
            .withDescription("Specify number of threads to be used.").create("thread_num");
        options.addOption(threadNumber);

        Option opNumber = OptionBuilder.withArgName("op_number").hasArg()
            .withDescription("Specify number of operations to be performed by each thread.").create("op_number");
        options.addOption(opNumber);
        
        Option waitTime = OptionBuilder.withArgName("wait_time").hasArg()
                .withDescription("Specify wait time bettween operations.").create("wait_time");
        options.addOption(waitTime);

        return options;
    }

  /**
   * Method that prints parameters needed
   */
  private static void printUsage(){
    StringBuilder strUsageBuilder = new StringBuilder();
    strUsageBuilder.append("Parameters needed are:");
    strUsageBuilder.append("\n\t-op_name <OperationName>");
    strUsageBuilder.append("\n\t-thread_num <NumberOfThreadsToBeRun>");
    strUsageBuilder.append("\n\t-op_number <NumberOfOperationsToBeRunInsideEachThread>");
    strUsageBuilder.append("\n\t-wait_time <WaitTimeBetweenOperations>");
    getLog().info(strUsageBuilder.toString());
  }

  private static Logger getLog() {
    return LOG;
  }
}
