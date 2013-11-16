/**
 * 
 */
package com.eyllo.paprika.test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.eyllo.database.DataTierMongoDB;
import com.eyllo.utils.WebUtils;
import com.mongodb.DB;

/**
 * @author renatomarroquin
 *
 */
public class ContinuousTest {

  /** Test to be run */
  private int test2run;

  /** Collection name to be tested */
  private String testDbName;

  /** Mongo manager class. */
  private DataTierMongoDB mongo;
  private int testInterval;
  private int timeReload;
  private String scenarioUrl;
  private String geoTagsUrl;
  private String server;

  /** Object to help us log operation behavior. */
  private static final Logger LOG = Logger.getAnonymousLogger();

  /** String containing the class that the logger belongs to */
  private final String TAG = ContinuousTest.class.getName();

  /** Properties to be configured. */
  Properties properties;

  private final String TEST_NAME = "test_name";
  private final String TEST_INTERVAL = "test_interval";
  private final String TEST_DATABASE = "test_database";
  private final String RELOAD_CONF = "reload_interval";
  private final String GET_SCENARIO_URL = "getScenarioUrl";
  private final String GET_TAGS_URL = "getGeoTagsUrl";
  private final String SERVER = "server";

  /** Test types */
  private final String COLL_SCAN_TEST = "collectionScans";
  private final String GET_SCE_TEST = "getScenarios";
  private final String GET_GEOTAGS_TEST = "getGeoTags";
  private final String MULTI_REQS_TEST = "multipleReqs";

  public ContinuousTest() {
    //Handler consoleHandler = new ConsoleHandler();
    //consoleHandler.setLevel(Level.FINER);
    //Logger.getAnonymousLogger().addHandler(consoleHandler);
    //ConsoleHandler handler = new ConsoleHandler();
    // PUBLISH this level
    //handler.setLevel(Level.FINER);
    //LOG.addHandler(consoleHandler);
  }
  /**
   * @param args
   */
  public static void main(String args[]) {
    if (args == null || args.length < 1) {
      System.out.print("Argument missing. ");
      System.out.println("<ConfigurationFilePath>");
    }
    else {
      ContinuousTest ct = new ContinuousTest();
      boolean flag = true;
      Date date = new Date();
      Date dateReload = new Date();
      ct.loadConf(args[0]);
      while(flag) {
        //decide test to be run
        switch(ct.getTest2run()) {
          case 0:
            ct.testGetScenarios();
            break;
          case 1:
            ct.testColsFullScan();
            break;
          case 2:
            ct.testGetGeoTags();
            break;
          case 3:
            ct.testMultipleRequests();
            break;
          default:
            System.out.println("Test not defined.");
            System.out.println("Tests available are: <getScenarios|collectionScans|getGeoTags|multipleReqs>");
            flag = false;
        }
        //get configuration every 30 secs
        try {
          Thread.sleep(ct.getTestInterval()*1000);
          dateReload = new Date();
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        if ((dateReload.getTime() - date.getTime())/1000 > ct.getTimeReload()) {
          getLog().logp(Level.INFO, "Main","","Re-Loading configuration");
          ct.loadConf(args[0]);
          date = dateReload;
        }
      }
    }
  }

  public void testSingleRequest() {
    testGetScenarios();
  }

  public void testMultipleRequests() {
    testGetScenarios();
    testGetGeoTags();
  }

  public void testGetScenarios() {
    //DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    Date date = new Date();
    String result = "";
    //getLog().setLevel(Level.ALL);
    //getLog().logp(Level.INFO, TAG, "testGetScenarios.", "testGetScenarios. InitTime: "+ dateFormat.format(date));
    try {
      result = WebUtils.runReadOperation(getScenarioUrl());
      //System.out.println(result);
    } catch (IOException e) {
      getLog().logp(Level.SEVERE, TAG,"testGetScenarios.", "Error Performing testGetScenarios ");
      getLog().logp(Level.SEVERE, TAG, e.getMessage(),"");
    }
    //http://mob-ppk-us-e.cloudapp.net/mobile?param={scenarioId:1,action:1413,social=1,radius:1000,lat:-22.904562,lng:-43.181677,%20version:%221306%22}
    Date date2 = new Date();
    //getLog().logp(Level.INFO, TAG, "testGetScenarios.", "testGetScenarios. EndTime:" + dateFormat.format(date2));
    if (!result.contains("Error response"))
      result ="testGetScenarios. TotalTestTime: " + String.valueOf((date2.getTime() - date.getTime())/1000);
    getLog().logp(Level.INFO, TAG, "testGetScenarios.", "testGetScenarios. " + result);
  }

  public void testGetGeoTags() {
    Date date = new Date();
    String result = "";
    //getLog().logp(Level.INFO, TAG, "testGetGeoTags.", "testGetGeoTags. InitTime:" + dateFormat.format(date));
    try {
      result = WebUtils.runReadOperation(getGeoTagsUrl());
    } catch (IOException e) {
      getLog().logp(Level.SEVERE, TAG,"Error Performing testGetGeoTags ", "");
      getLog().logp(Level.SEVERE, TAG, e.getMessage(),"");
    }
    //mobile.paprikamix.me/mobile?param={action:1413,scenarioId:5,lat:-25,lng:-42,radius:100000000, version:"0", maxGeotags:50}
    Date date2 = new Date();
    //getLog().logp(Level.INFO, TAG, "testGetGeoTags.",  "testGetGeoTags. EndTime:" + dateFormat.format(date2));
    if (!result.contains("Error response"))
      result = "testGetGeoTags. TotalTestTime: " + String.valueOf((date2.getTime() - date.getTime())/1000);
    getLog().logp(Level.INFO, TAG, "testGetGeoTags.", "testGetGeoTags. " + result);
  }

  /**
   * Tests full scans on top of all collections of a database.
   */
  public void testColsFullScan() {
    getLog().logp(Level.INFO, TAG,"Running collection full scan on " + getTestDbName(),"");
    if (getTestDbName() != null && !getTestDbName().equals("")) {
      try {
        DB ppkDb = mongo.returnDB(getTestDbName());
        Set<String> colls = ppkDb.getCollectionNames();
        for (String s : colls) {
          mongo.collFullScan("paprika", s);
        }
      } catch (Exception e) {
        getLog().logp(Level.SEVERE, TAG,"Error Performing Full Scan on database " + getTestDbName(),"");
        getLog().logp(Level.SEVERE, TAG, e.getMessage(),"");
        e.printStackTrace();
      }
    } else {
      getLog().logp(Level.SEVERE, TAG, "No collection name defined to perform Full Scan on it.","");
    }
    getLog().logp(Level.SEVERE, TAG, "Finished running collection full scan on " + getTestDbName(),"");
  }

  private void loadConf(String confPath) {
    properties = new Properties();
    try {
      properties.load(new FileInputStream(confPath));
      setTest2run(properties.get(TEST_NAME).toString());
      setTestInterval(Integer.parseInt(properties.get(TEST_INTERVAL).toString()));
      setTestDbName(properties.get(TEST_DATABASE).toString());
      setTimeReload(Integer.parseInt(properties.get(RELOAD_CONF).toString()));
      setScenarioUrl(properties.get(GET_SCENARIO_URL).toString());
      setGeoTagsUrl(properties.get(GET_TAGS_URL).toString());
      setServer(properties.get(SERVER).toString());
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
   }
  /**
   * @return the test2run
   */
  public int getTest2run() {
    return test2run;
  }
  /**
   * @param test2run the test2run to set
   */
  public void setTest2run(int test2run) {
    this.test2run = test2run;
  }
  /**
   * @param test2run the test2run to set
   */
  public void setTest2run(String test2run) {
    if (test2run.equals(GET_SCE_TEST))
      this.test2run = 0;
    else if (test2run.equals(COLL_SCAN_TEST))
      this.test2run = 1;
    else if (test2run.equals(GET_GEOTAGS_TEST))
      this.test2run = 2;
    else if (test2run.equals(MULTI_REQS_TEST))
      this.test2run = 3;
    else
      this.test2run = -1;
  }
  /**
   * @return the log
   */
  public static Logger getLog() {
    return LOG;
  }
  /**
   * @return the testDbName
   */
  public String getTestDbName() {
    return testDbName;
  }
  /**
   * @param testDbName the testDbName to set
   */
  public void setTestDbName(String testDbName) {
    this.testDbName = testDbName;
  }
  /**
   * @return the testInterval
   */
  public int getTestInterval() {
    return testInterval;
  }
  /**
   * @param testInterval the testInterval to set
   */
  public void setTestInterval(int testInterval) {
    this.testInterval = testInterval;
  }
  /**
   * @return the timeReload
   */
  public int getTimeReload() {
    return timeReload;
  }
  /**
   * @param timeReload the timeReload to set
   */
  public void setTimeReload(int timeReload) {
    this.timeReload = timeReload;
  }
  /**
   * @return the scenarioUrl
   */
  public String getScenarioUrl() {
    return scenarioUrl;
  }
  /**
   * @param scenarioUrl the scenarioUrl to set
   */
  public void setScenarioUrl(String scenarioUrl) {
    this.scenarioUrl = scenarioUrl;
  }
  /**
   * @return the geoTagsUrl
   */
  public String getGeoTagsUrl() {
    return geoTagsUrl;
  }
  /**
   * @param geoTagsUrl the geoTagsUrl to set
   */
  public void setGeoTagsUrl(String geoTagsUrl) {
    this.geoTagsUrl = geoTagsUrl;
  }
  /**
   * @return the server
   */
  public String getServer() {
    return server;
  }
  /**
   * @param server the server to set
   */
  public void setServer(String server) {
    this.server = server;
  }
}
