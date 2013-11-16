/**
 * 
 */
package com.eyllo.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author renatomarroquin
 *
 */
public class WebUtils {

  /** Object to help us log operation behavior */
  private static final Logger LOG = LoggerFactory.getLogger(WebUtils.class);

  /**
   * Generates readOperations to a specific url
   * @throws IOException 
   */
  public static String runReadOperation(String pUrl) throws IOException{
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    URLConnection conn=null;
    byte[] buf = new byte[4096];

    try {
        URL a = new URL(pUrl);
        conn = a.openConnection();
        InputStream is = conn.getInputStream();
        int ret = 0;
        while ((ret = is.read(buf)) > 0) {
            os.write(buf, 0, ret);
        }
        // close the inputstream
        is.close();
        //int respCode = ((HttpURLConnection)conn).getResponseCode();
        // check for error
        //if (respCode > 200) {
        //  throw new IOException("Error while getting page.");
       // }
        return new String(os.toByteArray());
    } catch (IOException e) {
        try {
            int respCode = ((HttpURLConnection)conn).getResponseCode();
            InputStream es = ((HttpURLConnection)conn).getErrorStream();
            int ret = 0;
            // read the response body
            while ((ret = es.read(buf)) > 0) {
                os.write(buf, 0, ret);
            }
            // close the errorstream
            es.close();
            return "Error response " + respCode + ": " + 
               new String(os.toByteArray());
        } catch(IOException ex) {
            throw ex;
        }
    }
  }

  /**
   * @return the log
   */
  public static Logger getLogger() {
    return LOG;
  }
}
