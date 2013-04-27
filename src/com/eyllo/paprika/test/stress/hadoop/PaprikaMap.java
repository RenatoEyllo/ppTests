package com.eyllo.paprika.test.stress.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Paprika mapper class
 * @author renatomarroquin
 *
 */
public class PaprikaMap extends Mapper<LongWritable , Text, Text, Text> {

  private static String WRITE_PARAMS = "%7Baction:1310,accessToken=%22AAFFRRDDFFSFF%22,geotag:%7B%22title%22:%22teste%22,%22text%22:%22teste%22,%22infobox%22:%7B%22title%22:%22teste%22,%22text%22:%22asfdasfasdfsdafasdf%22%7D,%22location%22:%7B%22lng%22:coorParam1,%22lat%22:coorParam2%7D,%22image%22:%7B%22id%22:%220%22,%22mime%22:%22%22%7D,%22type%22:%22text%22,%22uncodedTitle%22:%22teste%22,%22uncodedText%22:%22teste%22,%22fbUserId%22:123456789,%22fbChecked%22:false,%22userId%22:1,%22scenarioId%22:%221%22,%22id%22:%22%22,fbEmail:%22marta.silva@habanero.com%22,fbFstName=%22Marta%22,fbLstName=%22Silva%22,fbUserName=%22tt%22%7D%7D";
  private static String READ_PARAMS = "-C 'scenarioId=1' -C 'radius=10000' -C 'll=-22.920734855121413,-43.2698221941406'";
  private static String IMP_LINES = " 2>&1 | tail -30"; // 2>&1 
  private static String CMD = "ab -A tomcat:tomcat -b 1024 ";
  private static String SERVER = "http://serverParam:8080/servletMobile/mobile?param=";
  private static String CONC_PROC = "-c concurrencyParam -n processesParam -r -v 4 ";
  //private static String OUTPUT_FILE = "_output.distributed.micro.mongosharded.loadbalancer.log";

  private String concurrency;
  private String processes;
  private String server;
  private String taskname;
  private static HashMap<Text, FloatWritable>localCache;
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    localCache = new HashMap<Text, FloatWritable>();
  }

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
  {
    String operation = "", command = "";
    Configuration conf = context.getConfiguration();
    
    taskname = context.getTaskAttemptID().getTaskID().toString();
    concurrency = conf.get("concurrency");
    processes = conf.get("nprocesses");
    server = conf.get("server");
    operation = conf.get("operation");
    command = operation.equals("read")?getReadCmd():getWriteCmd();
    
    commandExecute(command.toString(), context);
  }

  @SuppressWarnings("rawtypes")
  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    Iterator it = localCache.entrySet().iterator();
    StringBuilder strAvg = new StringBuilder();
    context.write(new Text(taskname), new Text("Averaging: " + String.valueOf(PaprikaJobExecutor.NUM_PROCESS)));
    while (it.hasNext()) {
      Map.Entry pairs = (Map.Entry)it.next();
      strAvg.append(pairs.getKey() + " = ");
      strAvg.append(String.valueOf(Float.parseFloat(pairs.getValue().toString()) / PaprikaJobExecutor.NUM_PROCESS));
      context.write(new Text(taskname), new Text(strAvg.toString()));
      strAvg.setLength(0);
      it.remove(); // avoids a ConcurrentModificationException
    }
  }

  /**
   * Builds the read command to be executed
   * @return the Apache AB read command
   */
  private String getReadCmd(){
    StringBuilder cmdRead = new StringBuilder();
    cmdRead.append(CMD);
    cmdRead.append(CONC_PROC.replaceFirst("processesParam", processes));
    cmdRead.append(CONC_PROC.replaceFirst("concurrencyParam", concurrency));
    cmdRead.append(SERVER.replaceFirst("serverParam", server)).append(READ_PARAMS);
    cmdRead.append(IMP_LINES);//.append(outFile);
    return cmdRead.toString();
  }

  /**
   * Builds the write command to be executed
   * @return the Apache AB write command
   */
  private String getWriteCmd(){
    StringBuilder cmdRead = new StringBuilder();
    Random rand = new Random();

    cmdRead.append(CMD);
    cmdRead.append(CONC_PROC.replaceFirst("processesParam", processes).replaceFirst("concurrencyParam", concurrency));
    cmdRead.append(SERVER.replaceFirst("serverParam", server));
    cmdRead.append(WRITE_PARAMS.replace("coorParam1", String.valueOf(rand.nextInt(180) + 1)).replace("coorParam2",String.valueOf(rand.nextInt(180) + 1)));
    //cmdRead.append(IMP_LINES);//.append(outFile);
    return cmdRead.toString();
  }
  
  private void commandExecute(String pCommand, Context context) throws InterruptedException{
    String line = "";

    try {
      Runtime r = Runtime.getRuntime();
      Process p = r.exec(pCommand);
      p.waitFor();
      
      //while ((line = b.readLine()) != null) { //  if (isRelevant(line)) //    System.out.println(line); //}

      Scanner s = new Scanner(p.getInputStream());
      while (s.hasNextLine()) {
          line = s.nextLine();
          aggregateLine(line);
      }

    } catch (IOException e) {
      System.err.println("Error while executing the command " + pCommand);
      e.printStackTrace();
    }
  }

  /**
   * Gets the value from a result line but considering the specific position where it is located at
   * @param pLine
   * @param pPos
   * @return
   */
  private FloatWritable getValue(String pLine, int pPos){
    String []values =pLine.replaceAll("  +", "").replace(":", " ").split(" ");
    return new FloatWritable(Float.parseFloat(values[pPos]));
  }

  /**
   * Method that determines if a line is important for us or not
   * @param pLine
   * @return
   */
  private void aggregateLine(String pLine){
    FloatWritable value, initValue;
    if (pLine.contains("Concurrency Level:")){
      value = getValue(pLine, 2);
      initValue = localCache.get(new Text("Concurrency Level:"));
      if (initValue == null)
        initValue = new FloatWritable(0);
      value.set(initValue.get() + value.get());
      localCache.put(new Text("Concurrency Level:"), value);
    }
    else if (pLine.contains("Complete requests:")){
      value = getValue(pLine, 2);
      initValue = localCache.get(new Text("Complete requests:"));
      if (initValue == null)
        initValue = new FloatWritable(0);
      value.set(initValue.get() + value.get());
      localCache.put(new Text("Complete requests:"), value);
    }
    else if (pLine.contains("Time taken for tests:")){
      value = getValue(pLine, 4);
      initValue = localCache.get(new Text("Time taken for tests:"));
      if (initValue == null)
        initValue = new FloatWritable(0);
      value.set(initValue.get() + value.get());
      localCache.put(new Text("Time taken for tests:"), value);
    }
    else if (pLine.contains("Requests per second:")){
      value = getValue(pLine, 3);
      initValue = localCache.get(new Text("Requests per second:"));
      if (initValue == null)
        initValue = new FloatWritable(0);
      value.set(initValue.get() + value.get());
      localCache.put(new Text("Requests per second:"), value);
    }
    else if (pLine.contains("(mean, across all concurrent requests)")){
      value = getValue(pLine, 3);
      initValue = localCache.get(new Text("(mean, across all concurrent requests)"));
      if (initValue == null)
        initValue = new FloatWritable(0);
      value.set(initValue.get() + value.get());
      localCache.put(new Text("(mean, across all concurrent requests)"), value);
    }
    else if (pLine.contains("longest request")){
      value = getValue(pLine, 1);
      initValue = localCache.get(new Text("longest request:"));
      if (initValue == null)
        initValue = new FloatWritable(0);
      value.set(initValue.get() + value.get());
      localCache.put(new Text("longest request:"), value);
    }
  }
}
