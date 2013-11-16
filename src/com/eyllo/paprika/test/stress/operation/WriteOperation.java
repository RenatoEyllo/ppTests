package com.eyllo.paprika.test.stress.operation;

import java.net.URL;
import java.net.URLConnection;

import javax.servlet.ServletException;

import com.eyllo.database.DataTierMongoDB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * Class in charged of the put operation for Paprika Application
 * @author renatomarroquin
 *
 */
public class WriteOperation extends AbstractOperation{

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
  private static DataTierMongoDB mongoAccess;
  
  /**
   * Application parameters
   */
  private static String PARAMETERS = "scenarioId=1&radius=10000&ll=-22.920734855121413,-43.2698221941406";
  
  /**
   * Operation name
   */
  private static String operationName = "WriteOperation";

  static{
    try {
      mongoAccess = new DataTierMongoDB();
    } catch (ServletException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  /**
   * Default constructor for the PutOperation
   */
  public WriteOperation(){
    super(operationName, 0, 0);
  }

  @Override
  public float doProcessing() throws InterruptedException {
    getLogger().info("Starting processing for " + getName());
    long startTime, endTime;
    long avgTime = 0;
    
    startTime = System.nanoTime();
    DBCollection tmpCollection = mongoAccess.getCollection("eyllo_test", "users");
    
    DBCursor results = tmpCollection.find();
    for(DBObject obj : results){
      System.out.println(obj.toString());
    }
    
    /*for (int iCnt = 0; iCnt < this.getOperations(); iCnt++){
      getLogger().debug("Reading from application: "+ Integer.toString(iCnt + 1));
      Thread.sleep(this.getWaitTime());
      startTime = System.nanoTime();
      runReadOperation();
      endTime = System.nanoTime();
      avgTime += endTime - startTime;
    }*/
    
    endTime = System.nanoTime();
    avgTime += endTime - startTime;
    
    return avgTime/this.getOperations();
  }

  /**
   * Generates readOperations to a specific url
   */
  private void runWriteOperation(){
    
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
