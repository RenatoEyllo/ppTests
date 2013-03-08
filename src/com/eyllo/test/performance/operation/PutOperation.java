package com.eyllo.test.performance.operation;

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
public class PutOperation extends AbstractOperation{

    private static String URLSTRING="url";
    private URL url;
    private URLConnection urlConnection;
    
  /**
   * Number of write operations to be performed
   */
  private int writeOperations = 10000;

  /**
   * Operation name
   */
  private static String operationName = "PutOperation";

  /**
   * Default constructor for the PutOperation
   */
  public PutOperation(){
    super(operationName);
  }

  @Override
  public float doProcessing() throws InterruptedException {
    long startTime = System.nanoTime();
    // TODO Connect to application and writing
    for (int iCnt = 0; iCnt < this.writeOperations; iCnt++){
        runReadOperation();
        System.out.println("Writing to application: "+ Integer.toString(iCnt));
    }
    long endTime = System.nanoTime();
    
    return endTime - startTime;
  }

  private void runReadOperation(){
      try {
            url = new URL(URLSTRING);
            try {
                byte[] readBuffer = new byte[20]; 
                urlConnection = url.openConnection();
                InputStream inputStream = urlConnection.getInputStream() ;
                String StringFromInputStream = IOUtils.toString(inputStream, "UTF-8");
                System.out.println(StringFromInputStream);
               
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
  }
  
  public int getWriteOperations() {
    return writeOperations;
  }

  public void setWriteOperations(int writeOperations) {
    this.writeOperations = writeOperations;
  }

}
