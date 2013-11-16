package com.eyllo.paprika.test.stress.hadoop;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *  MapReduce controller for executing Apache AB in a distributed manner.
 */
public class PaprikaJobExecutor {

  /**
   * Variables needed to set the hadoop job up
   */
  private String inputPath;
  private String outputPath;
  private String appServer;
  private String operation;
  private String concurrency;
  private String processes;
  private String split;

  /**
   * Number of processes within a Mapper i.e. number of lines per mapper
   */
  public static int NUM_PROCESS = 10;

  /**
   * Default constructor
   */
  PaprikaJobExecutor(){
    this.initialize("", "", "", "", "", "", "");
  }

  /**
   * Constructor
   * @param pInputPath
   * @param pOutputPath
   * @param pAppServer
   * @param pConcurrency
   * @param pProcesses
   * @param pSplit
   */
  PaprikaJobExecutor(String pInputPath, String pOutputPath, String pAppServer, String pOperation, String pConcurrency, String pProcesses, String pSplit){
    this.initialize(pInputPath, pOutputPath, pAppServer, pOperation, pConcurrency, pProcesses, pSplit);
  }

  /**
   * Method in charged of setting the parameters up
   * @param pInputPath
   * @param pOutputPath
   * @param pAppServer
   * @param pConcurrency
   * @param pProcesses
   * @param pSplit
   */
  private void initialize(String pInputPath, String pOutputPath, String pAppServer, String pOperation, String pConcurrency, String pProcesses, String pSplit){
    this.setInputPath(pInputPath);
    this.setOutputPath(pOutputPath);
    this.setAppServer(pAppServer);
    this.setOperation(pOperation);
    this.setConcurrency(pConcurrency);
    this.setProcesses(pProcesses);
    this.setSplit(pSplit);
  }

  /**
   * Main program using the following parameters:
   *    args[0] = input
   *    args[1] = output
   *    args[2] = server
   *    args[3] = concurrency
   *    args[4] = process
   *    args[5] = split
   *    
   *    n 100 c 50
   *    Procesos: 100 * ( n ) = 10 000 -> 500 concurrentes
   *    Mappers: 20 * ( procesos ) = 200 000
   **/
  public static void main (String[] args) throws Exception  {
    if(args.length > 7){
      System.err.println("Wrong number of arguments for executing this.");
      return;
    }

    PaprikaJobExecutor executor = new PaprikaJobExecutor(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
    Configuration conf = new Configuration();
    Job job =null;

    // definir este parâmetro quando o número linhas por processos seja muito grande. 
    conf.setLong("mapred.task.timeout", 900000000);
    try {
      job = new Job(conf, "PaprikaJobExecutor");
    } catch (IOException e2) {
      System.err.println("Error trying to instanciate the JobConfiguration object");
      e2.printStackTrace();
    }

    job.getConfiguration().setStrings("concurrency", executor.getConcurrency());
    job.getConfiguration().setStrings("nprocesses", executor.getProcesses());
    job.getConfiguration().setStrings("operation", executor.getOperation());
    job.getConfiguration().setStrings("server", executor.getAppServer());

    // Setting MapReduce class parameters
    job.setJarByClass(PaprikaJobExecutor.class);
    job.setMapperClass(PaprikaMap.class);
    //job.setReducerClass(PaprikaReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    int numberProcess = NUM_PROCESS;
    int numberSplit = Integer.parseInt(executor.getSplit());
    NThreadInputFormat.setNumberOfProcess(job, numberProcess);
    NThreadInputFormat.setNumberOfSplits(job, numberSplit);
    job.setInputFormatClass(NThreadInputFormat.class);
    
    // Defining input path
    FileInputFormat.addInputPath(job, new Path(executor.getInputPath()));
    executor.verifyOutputPath(new File(executor.getOutputPath()));
    FileOutputFormat.setOutputPath(job, new Path(executor.getOutputPath()));

    // Starting and waiting process
    System.exit(job.waitForCompletion(true) ? 0 : 1); 	
  }

  /**
   * Verifies if the output path exists, and deletes if it does
   * @param directory
   */
  private void verifyOutputPath(File directory){
    if (directory.exists()){
      File[] files = directory.listFiles();
      if(null!=files){
          for(int i=0; i<files.length; i++) {
            if(files[i].isDirectory())
              verifyOutputPath(files[i]);
            else 
              files[i].delete();
          }
      }
    }
    directory.delete();
  }
  
  /**
   * @return the inputPath
   */
  public String getInputPath() {
    return inputPath;
  }

  /**
   * @param inputPath the inputPath to set
   */
  public void setInputPath(String inputPath) {
    this.inputPath = inputPath;
  }

  /**
   * @return the outputPath
   */
  public String getOutputPath() {
    return outputPath;
  }

  /**
   * @param outputPath the outputPath to set
   */
  public void setOutputPath(String outputPath) {
    this.outputPath = outputPath;
  }

  /**
   * @return the appServer
   */
  public String getAppServer() {
    return appServer;
  }

  /**
   * @param appServer the appServer to set
   */
  public void setAppServer(String appServer) {
    this.appServer = appServer;
  }

  /**
   * @return the concurrency
   */
  public String getConcurrency() {
    return concurrency;
  }

  /**
   * @param concurrency the concurrency to set
   */
  public void setConcurrency(String concurrency) {
    this.concurrency = concurrency;
  }

  /**
   * @return the processes
   */
  public String getProcesses() {
    return processes;
  }

  /**
   * @param processes the processes to set
   */
  public void setProcesses(String processes) {
    this.processes = processes;
  }

  /**
   * @return the split
   */
  public String getSplit() {
    return split;
  }

  /**
   * @param split the split to set
   */
  public void setSplit(String split) {
    this.split = split;
  }

  /**
   * @return the operation
   */
  public String getOperation() {
    return operation;
  }

  /**
   * @param operation the operation to set
   */
  public void setOperation(String operation) {
    this.operation = operation;
  }
}
