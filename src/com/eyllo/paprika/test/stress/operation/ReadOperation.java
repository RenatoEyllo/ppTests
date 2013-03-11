package com.eyllo.paprika.test.stress.operation;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.io.IOUtils;

/**
 * Class in charged of the put operation for Paprika Application
 * @author renatomarroquin
 *
 */
public class ReadOperation extends AbstractOperation{

  /**
   * Server connection objects
   */
  private URL url;
  private URLConnection urlConnection;
  
  /**
   * Server address
   */
  private static String SERVER = "http://ec2-184-73-121-5.compute-1.amazonaws.com";
  
  /**
   * Server port
   */
  private static String PORT = "8080";
  
  /**
   * Application path
   */
  private static String PATH = "paprika.json";
  
  /**
   * Application parameters
   */
  private static String PARAMETERS = "scenarioId=1&radius=10000&ll=-22.920734855121413,-43.2698221941406";
  
  /**
   * Operation name
   */
  private static String operationName = "ReadOperation";

  /**
   * Default constructor for the PutOperation
   */
  public ReadOperation(){
    super(operationName, 0, 0);
  }

  @Override
  public float doProcessing() throws InterruptedException {
    getLogger().info("Starting processing for " + getName());
    long startTime = System.nanoTime();
    for (int iCnt = 0; iCnt < this.getOperations(); iCnt++){
      getLogger().debug("Reading from application: "+ Integer.toString(iCnt + 1));
      runReadOperation();
    }
    long endTime = System.nanoTime();
    
    return endTime - startTime;
  }

  /**
   * Generates readOperations to a specific url
   */
  private void runReadOperation(){
    try {
      this.url = new URL(getURLSTRING());
      this.urlConnection = this.url.openConnection();
      InputStream inputStream = this.urlConnection.getInputStream() ;
      String StringFromInputStream = IOUtils.toString(inputStream, "UTF-8");
      getLogger().debug("Answer: " + StringFromInputStream);
    } catch (MalformedURLException e) {
      getLogger().error("Error while performing readOperations");
      e.printStackTrace();
    } catch (IOException e) {
      getLogger().error("Error while performing readOperations");
      e.printStackTrace();
    }
  }
  

  /**
   * Gets formed application URL
   * @return
   */
  public static String getURLSTRING() {
    return getServer() + ":" + PORT + "/" + PATH + "?" + PARAMETERS;
  }

  /**
   * Getter for the Server where operation will be performed
   * @return
   */
  public static String getServer() {
    return SERVER;
  }

  /**
   * Setter for the Server where operation will be performed
   * @return
   */
  public static void setServer(String pServer) {
    SERVER = pServer;
  }

}
