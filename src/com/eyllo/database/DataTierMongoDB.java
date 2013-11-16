/**
 * PRIVATE AND CONFIDENTIAL - Copyright (C) 2012, 2013 - eyllo tecnologia  
 *           www.eyllo.com
 *           
 * DataTierMongoDB.java (this file) - Java source file (.java)
 * 
 * Abstracts access to MongoDB. Implements a n-tier architecture. 
 * 
 *********************** SERVLETS ***************************
 * 
 * Author Enylton
 * Date:
 * Modified: Roberto
 * Last date: October, 17, 2013
 */

package com.eyllo.database;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.UnavailableException;

import org.json.JSONObject;

import com.eyllo.utils.ConnectionString;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

public class DataTierMongoDB { // ********* USE SERVLET-MOBILE REPOSITORY FOR THIS CLASS ***********

  private MongoClient mongo;      // a MongoDB connection
  private HashMap<String, DB> dbMap;  // a Map with a list of databases
  
  private final String TAG = DataTierMongoDB.class.getName();
  private final static Logger logger = Logger.getLogger(DataTierMongoDB.class.getName()); // error logger

  public DataTierMongoDB() throws ServletException {
    try {
      //mongo = new MongoClient(ConnectionString.getStrServer(), ConnectionString.getStrPort());
      mongo = new MongoClient(ConnectionString.getStrServer(), ConnectionString.getStrPort());
        mongo.getMongoOptions().setAutoConnectRetry(true);
        mongo.getMongoOptions().setMaxWaitTime(100000);
        mongo.getMongoOptions().setSocketTimeout(100000);
        mongo.getMongoOptions().setConnectTimeout(100000);
      dbMap = new HashMap<String, DB>();
    } catch (java.net.UnknownHostException e){
      logger.logp(Level.SEVERE, TAG, "DataTierMongoDB", e.getMessage());
      e.printStackTrace();
      throw new UnavailableException("Could not open a connection to the database", 20);
    }
    logger.setLevel(Level.ALL);
  }

    /**
     * Performs a full scan on top of a collection
     * @param databse
     * @param collectionName
     */
    public void collFullScan(String databse, String collectionName) {
      DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
      Date date = new Date();
      logger.logp(Level.FINE, TAG, "collFullScan " + databse + collectionName + " InitTime:",
          dateFormat.format(date));
      DBCollection geoTags = getCollection(databse, collectionName);
      DBCursor cursor = geoTags.find();
      logger.logp(Level.FINE, TAG, "collFullScan " + databse + collectionName + " Size:",
          String.valueOf(cursor.count()));
      while(cursor.hasNext())
         cursor.next();
      Date date2 = new Date();
      logger.logp(Level.FINE, TAG, "collFullScan " + databse + collectionName + " EndTime:",
          dateFormat.format(date2));
    }

      /**
       * Gets a specific database
       * @param database
       * @return
       */
      public DB returnDB(String database) {
        return mongo.getDB(database);
      }

  /**
   * check if the database is running
   * @return
   */
  public boolean isRunning(){
    try{
      List<String> dbNames = mongo.getDatabaseNames();
      if(!dbNames.isEmpty()){
        return true;
      } else {
        return false;
      }
    } catch (MongoException e){
//      e.printStackTrace();
      return false;
    }
  }
  
  /**
   * Find the geotags with distance less than radius from lat, long.
   * 
   * @param scenarioName - The used scenario.
   * @param latitude - Current latitude.
   * @param longitude - Current longitude.
   * @param radius - Radius (in meters).
   * @return The result obtained from DB.
   
  public Vector<GeoTag> getGeotagArray(int scenarioId, double latitude, double longitude, double radius, String version, int maxGeotags) {
    try {
      // transform to radians
      double radiusInRadians  =  ((double) radius / 6371 / 1000);
      String url = "";
      DBCollection collection = getCollection("paprika", "geoTags");
      DBCollection scenarioCollection = getCollection("paprika", "scenariosList");
      BasicDBObject queryURLScenario = new BasicDBObject();
      queryURLScenario.put("scenarioId", scenarioId);
      
      BasicDBObject field = new BasicDBObject("url", true).append("_id", false);    
      DBCursor urlCursor = scenarioCollection.find(queryURLScenario, field).limit(1);
      
      while(urlCursor.hasNext()){
        DBObject urlTempArray = urlCursor.next();
        url = (String) urlTempArray.get("url");
        }
      String urlScenario = url.substring(0, url.lastIndexOf("/") + 1);
      
      BasicDBObject queryGeotags = new BasicDBObject();
      // mongoDB uses (longitude, latitude)
      BasicDBObject coord = new BasicDBObject();
      coord.put("lng", longitude);
      coord.put("lat", latitude);
      queryGeotags.put("loc", new BasicDBObject("$nearSphere", coord).append("$maxDistance", radiusInRadians));
      if (scenarioId != 0) {
        queryGeotags.put("scenarioId", scenarioId);
      }
      logger.logp(Level.INFO, TAG, "getGeotagArray", " query:" + queryGeotags.toString() + ", version:"+version);
      DBCursor cursor = collection.find(queryGeotags).limit(maxGeotags);
      return processGeotags(cursor, urlScenario);
    } catch (Exception e) {
      String msg = e.getMessage() + " latitude:" + latitude +  " longitude:" + longitude 
            +  " radius:" + radius +  " version:" + version + " scenario:" + scenarioId;
      logger.logp(Level.SEVERE, TAG, "getGeotagArray", msg);
      e.printStackTrace();
    }
    return null;
  }*/
  
  /**
   * Process the response coming from the DB.
   * 
   * @param cursor- The result received from the DB.
   * @return a {@link GeoTag} vector with the results from the DB.
   
  private Vector<GeoTag> processGeotags(DBCursor cursor, String url) {
    try {
      Vector<GeoTag> geotagVector = new Vector<GeoTag>();
      GeoTag geotag;
      DBObject object;
      
      while (cursor.hasNext()) {
        object = cursor.next();
        String TypeObj = (String) object.get("type");
        
        geotag = new GeoTag();
        geotag.setScenarioId(Long.valueOf(object.get("scenarioId").toString()));
        geotag.setUserId(Long.valueOf(object.get("userId").toString()));
        geotag.setGeotagId(object.get("_id").toString());
        if(object.containsField("fbUserId")){
          geotag.setFbUserId( Long.valueOf(object.get("fbUserId").toString()));        
        }
        geotag.setType( object.get("type").toString());
        geotag.setTitle(object.get("title").toString());
        geotag.setText( object.get("text").toString());
        if(object.containsField("loc")){
          BasicDBObject loc = (BasicDBObject) object.get("loc");
          if(loc.containsField("type")){
            if((loc.getString("type").compareTo("Point") == 0 ) && (loc.containsField("coordinates"))){
              JSONArray coordinates;
              try {
                coordinates = new JSONArray(JSON.serialize(loc.get("coordinates")));
                geotag.setLongitude(coordinates.getDouble(0));
                geotag.setLatitude(coordinates.getDouble(1));        
              } catch (JSONException e) {
                logger.logp(Level.SEVERE, TAG, "processGeotags", e.getMessage() + " object:" + object.toString());
                e.printStackTrace();
              }              
            }
          }
        }
        if(object.containsField("icon")){
          geotag.setIcon(object.get("icon").toString());
        }

        // if geotag has categoryId, query for icons, if not, set default
        if(object.containsField("categoryId")) {
          DBObject categoryTempArray = null;
          DBCollection collection = getCollection("paprika", "category");
          BasicDBObject queryCategory = new BasicDBObject();
          int categoryId = (Integer) object.get("categoryId");
          queryCategory.put("categoryId", categoryId);
          DBCursor cursorCategory = collection.find(queryCategory);
          while(cursorCategory.hasNext()){
            categoryTempArray = cursorCategory.next();
            geotag.setMobileIosGeotagIcon(url + categoryTempArray.get("iOSGeotagIcon").toString());
            geotag.setMobileIosMapIcon(url + categoryTempArray.get("iOSMapIcon").toString());
            geotag.setMobileAndroidGeotagIcon(url + categoryTempArray.get("androidGeotagIcon").toString());
            geotag.setMobileAndroidMapIcon(url + categoryTempArray.get("androidMapIcon").toString());
          }
        } else {
          DBObject categoryTempArray = null;
          DBCollection collection = getCollection("paprika", "category");
          BasicDBObject queryCategory = new BasicDBObject();
          int categoryId = (Integer) object.get("categoryId");
          queryCategory.put("categoryId", 1);
          DBCursor cursorCategory = collection.find(queryCategory);
          while(cursorCategory.hasNext()){
            categoryTempArray = cursorCategory.next();
            geotag.setMobileIosGeotagIcon(url + categoryTempArray.get("iOSGeotagIcon").toString());
            geotag.setMobileIosMapIcon(url + categoryTempArray.get("iOSMapIcon").toString());
            geotag.setMobileAndroidGeotagIcon(url + categoryTempArray.get("androidGeotagIcon").toString());
            geotag.setMobileAndroidMapIcon(url + categoryTempArray.get("androidMapIcon").toString());
          }
        }
        if(object.containsField("licon")) {
          geotag.setLicon(object.get("licon").toString());
        }
        
        if((TypeObj.equalsIgnoreCase("text") ) && (object.containsField("infobox")) ) {
          DBObject infobox = (DBObject)object.get("infobox");
          geotag.setInfobox(infobox.get("title").toString(), infobox.get("text").toString());
        }
        if(TypeObj.equalsIgnoreCase("image") ) {
          DBObject image = (DBObject)object.get("image");
          Long size = 0L;
          if(image.containsField("size")){
            size = Long.valueOf(image.get("size").toString());
          }
          geotag.setMedia(image.get("id").toString(), image.get("mime").toString(), size);
        }
        if(TypeObj.equalsIgnoreCase("audio") ) {
          DBObject audio = (DBObject)object.get("audio");
          Long size = 0L;
          if(audio.containsField("size")){
            size = Long.valueOf(audio.get("size").toString());
          }
          geotag.setMedia(audio.get("id").toString(), audio.get("mime").toString(), size);
        }
        if(TypeObj.equalsIgnoreCase("video") ) {
          DBObject video = (DBObject)object.get("video");
          Long size = 0L;
          if(video.containsField("size")){
            size = Long.valueOf(video.get("size").toString());
          }
          geotag.setMedia(video.get("id").toString(), video.get("mime").toString(), size);
        }
        // add another geoTag to the vector
        geotagVector.add(geotag);
      }
      return geotagVector;
    } catch (Exception e) {
      logger.logp(Level.SEVERE, TAG, "processGeotags", e.getMessage() + " object:" + cursor.toString());
      e.printStackTrace();
    }
    return null;
  }*/

  /** 
   * gets a list of scenarios from the database
   * @param mobileLat
   * @param mobileLng
   * @param version
   * @return
   
  public Vector<Scenario> getScenarioList(double mobileLat, double mobileLng, String version) {
    try {
      BasicDBObject scenarioOrder = new BasicDBObject();
      DBCollection collection = getCollection("paprika", "scenariosList");
      // converting Km to radians (the first number is the radius range in Km. The second, 6371,  is the convertion factor).
      double radius= 200000 / 6371;
      List<Object> center = new ArrayList<Object>();
      center.add(new double [] {mobileLng, mobileLat});
      center.add(radius);
      // queries for the scenarios names
      BasicDBObject queryScenarios = new BasicDBObject("loc", new BasicDBObject("$geoWithin", new BasicDBObject("$centerSphere", center)));
      scenarioOrder.put("scenarioOrder", 1);
      logger.logp(Level.INFO, TAG, "getScenarioList", " queryScenarios:" + queryScenarios.toString() + ", orderScenarioId:" + scenarioOrder.toString() + ", version:"+version);
      DBCursor cursor = collection.find(queryScenarios).sort(scenarioOrder);
//      Vector<Scenario> tempo = processScenarioList(cursor, mobileLat, mobileLng, version);
//      for(Scenario scenario : temp){
//        scenarioArray.put(scenario.toJSONObject());
//      }
      return processScenarioList(cursor, mobileLat, mobileLng, version);
      
      
    } catch (Exception e) {
      String msg = e.getMessage() + " latitude:" + mobileLat +  " longitude:" + mobileLng +  " version:" + version;
      logger.logp(Level.SEVERE, TAG, "getScenarioList", msg);
      e.printStackTrace();
    }
    return null;
  } */

  /**
   * process the result from the database and returns a vector with a list of scenarios
   * @param cursor
   * @param mobileLat
   * @param mobileLng
   * @param mobileVersion
   * @return
   
  private Vector<Scenario> processScenarioList(DBCursor cursor, double mobileLat, double mobileLng, String mobileVersion) {
    try {
      Vector<Scenario> scenarioVector = new Vector<Scenario>();
      BasicDBObject query;
      String mobileIosIcon = "";
      int compareVersion = 0, compareDate = 0;
      Date validDate;
      int radius, gridCounter = 1;
      double lat = 0, lng = 0;
      DBObject object, location;
        BasicDBObject queryGrid = new BasicDBObject(); 
        BasicDBObject fieldGrid = new BasicDBObject("imageSize", true).append("_id", false);
      DBCollection collection = getCollection("paprika", "appVersion");
      DBCollection mobileScenarioGridCollection = getCollection("paprika", "mobileScenarioGrid");
      int mobileScenarioGridSize = mobileScenarioGridCollection.find().count();
      Scenario scenario;
      while(cursor.hasNext()){
        object = cursor.next();
        // verifies if the scenario is active
        boolean active = (Boolean) object.get("active");
        if (!active){
          continue;
        }
        // verifies if scenario is valid 
        validDate = (Date) object.get("validate");
        Date today = new Date();
        compareDate = validDate.compareTo(today);
        if(compareDate > 0){
          continue; // this scenario will be valid in the future
        }
        // retrieves the version name
        int versionId = (Integer) object.get("versionId");
        query = new BasicDBObject();
        query.put("versionId", versionId);
        DBObject versionCursor = collection.findOne(query);
        String versionName = (String) versionCursor.get("versionName");
        // compares the versions (mobile x DB)
        compareVersion = versionName.compareTo(mobileVersion);      
        // if scenario version <= mobile version, it is loaded
        if(compareVersion > 0 ){
          continue;
        }
        int scenarioOrder = (Integer) object.get("scenarioOrder");
        int scenarioId = (Integer) object.get("scenarioId");
        String scenarioName =  (String) object.get("nameScenario");
        String url = ((String) object.get("url"));
        String urlScenario = url.substring(0, url.lastIndexOf("/") + 1);
        String mobileAndroidIcon =  urlScenario + (String) object.get("mobileAndroidIcon");
        location = (DBObject) object.get("location");
        lng = (double) location.get("lng");
        lat = (double) location.get("lat");
        radius = (int) object.get("radius");
        // if NaN, next
        double theta = mobileLng - lng;
            // calculating the distance from the mobile position to the center of scenario.
            double distance = Math.sin(degreeToRad(mobileLat)) * Math.sin(degreeToRad(lat)) + Math.cos(degreeToRad(mobileLat)) * Math.cos(degreeToRad(lat)) * Math.cos(degreeToRad(theta));
            distance = Math.acos(distance);
            distance = radToDegree(distance);
            // distance in Km
            distance = distance * 60 * 1.609344;
            // verifies if the mobile is on the range of scenario
            if (distance <= radius || radius == 0){
              
              // verifies the scenario order and image size for image grid on iOS mobile
              if (gridCounter > mobileScenarioGridSize){
                gridCounter = 1;
              }
              queryGrid.put("gridPosition", gridCounter);
              DBObject scenarioImageSizeDB = mobileScenarioGridCollection.findOne(queryGrid, fieldGrid); 
          String scenarioImageSize = scenarioImageSizeDB.get("imageSize").toString();
              if (scenarioImageSize.equalsIgnoreCase("big")){
                mobileIosIcon =  urlScenario + (String) object.get("mobileIosIconBig");
              } else if (scenarioImageSize.equalsIgnoreCase("medium")){
                mobileIosIcon =  urlScenario + (String) object.get("mobileIosIconMedium");
              } else {
                mobileIosIcon =  urlScenario + (String) object.get("mobileIosIcon");
              }
              gridCounter = gridCounter + 1;

              scenario = new Scenario();
          scenario.setOrder(scenarioOrder);
          scenario.setId(scenarioId);
          scenario.setName(scenarioName);
          scenario.setLongitude(lng);
          scenario.setLatitude(lat);            
          scenario.setRadius(radius);
          scenario.setUrl(url);
          scenario.setMobileIosIcon(mobileIosIcon);
          scenario.setMobileAndroidIcon(mobileAndroidIcon);
          scenario.setVersionName(versionName);                
          // add another scenario to the vector
          scenarioVector.add(scenario);
            }
      } 
      return scenarioVector;
    } catch (Exception e) {
      String msg = e.getMessage() + " latitude:" + mobileLat +  " longitude:" + mobileLng +  " version:" + mobileVersion + " cursor:" + cursor;
      logger.logp(Level.SEVERE, TAG, "processScenarioList", msg);
      e.printStackTrace();
    }
    return null;
  }*/
  
  /**
   * Retrieves the last scenario used by the user
   * 
   * @param userId
   * @return
   */
  public int lastUsedScenario(int userId){
    try {
      // verifies the last scenario used by the user
      int lastScenarioId = 1;
      DBCollection usersCollection = getCollection("paprika", "users");
      BasicDBObject userQuery = new BasicDBObject();
      userQuery.put("userId", userId);
      logger.logp(Level.INFO, TAG, "lastUsedScenario", " userQuery:" + userQuery.toString());
      List<DBObject> userLastScenario = (List<DBObject>) usersCollection.find(userQuery).toArray();
      if (userLastScenario.size() > 0) {
        try {
          lastScenarioId = (Integer) userLastScenario.get(0).get("scenarioId");
        } catch (NullPointerException e) {
          logger.logp(Level.SEVERE, TAG, "verifiesLastScenario", e.getMessage() + " user id" + userId);
          e.printStackTrace();
          lastScenarioId = 0;
        }
      };
      return lastScenarioId;
    } catch(Exception e){
      logger.logp(Level.SEVERE, TAG, "verifiesLastScenario", e.getMessage() + " user id" + userId);
      e.printStackTrace();
    }
    return 1; // if no last scenario is returned, uses scenarioId = 1 (paprika) 
  }
  
  
  /**
   * compress the database
   * @return
   */
  @SuppressWarnings("unused")
  public String compressDB(String database) {
    try {
      int counter = 0;
      StringBuilder result = new StringBuilder();
      BasicDBObject cmd = new BasicDBObject(); 
      Set<String> listCollection = getCollectionNames(database);
      DB db = getDB(database);
      for (String collection: listCollection){
        cmd.put("collStats", collection);
        CommandResult beforeCompact = db.command(cmd);
        cmd.put("compact", collection);
        CommandResult duringCompact = db.command(cmd);
        cmd.put("collStats", collection);
        CommandResult afterCompact = db.command(cmd);
        counter = counter + 1;
        result.append("Collection: " + collection + "\n");
        result.append("Before compact: " + beforeCompact + "\n");
        result.append("After  compact: " + afterCompact + "\n");
        result.append("---------------------------------------------- \n");
      }
      result.append("Returned number of documents: " + counter);
      return result.toString();
    } catch (Exception e) {
      logger.logp(Level.SEVERE, TAG, "compressDB", e.getMessage() + " database:" + database );
      e.printStackTrace();
    }
    return null;
  }

  /**
   * submit a command to the database
   * 
   * @param database - databse name
   * @param collection - collection name
   * @param query - query for doc search
   * @param operation - operation to be done in DB (insert, remove or update)
   * @param command - command
   * @param all - if true, all docs will be affected by the command
   * 
   * @return
   */
  public String submitDBCommand(String database, String collection, String query, String operation, String command, Boolean all) {
    WriteResult result = null;
    try {
      DBCollection DBCollection = getCollection(database, collection);
      if (operation.equals("insert")){
        JSONObject teste = new JSONObject(command);
        Object teste1 = JSON.parse(teste.toString());
        DBObject doc = (DBObject) teste1;
        result = DBCollection.save(doc);
      } else if (operation.equals("update")){
        DBObject queryDB = (DBObject) JSON.parse(query);
        DBObject doc = (DBObject) JSON.parse(command);
        if (all){
          result = DBCollection.update(queryDB, doc, false, true);
        } else {
          result = DBCollection.update(queryDB, doc);
        }
      } else if (operation.equals("remove")){
        DBObject queryDB = (DBObject) JSON.parse(query);
        DBObject doc = (DBObject) JSON.parse(command);
        if (all){
          result = DBCollection.update(queryDB, doc, false, true);
        } else {
          result = DBCollection.update(queryDB, doc);
        }
      }
      return "Result: " + result.toString();
    } catch (Exception e) {
      logger.logp(Level.SEVERE, TAG, "executeDBCommand", e.getMessage() + " database:" + database + " collection: " + collection + ", result: "  + result.toString());
      e.printStackTrace();
    }
    return null;
  } //database, collection, query, operation, command, all
  
  /**
   * return a collection list. 
   * @param database
   * @return collection (the list of collections)
   */
  public List<String> getDatabaseNames() {
  try {
    // get the database
    List<String> dbNames = mongo.getDatabaseNames();
    logger.logp(Level.INFO, TAG, "getCollectionNames", " databases:" + dbNames);
    return dbNames;
  } catch (Exception e) {
    logger.logp(Level.SEVERE, TAG, "getCollectionNames", e.getMessage());
    e.printStackTrace();
  }
  return null;
  }
  
  /**
   * return a collection list. 
   * @param database
   * @return collection (the list of collections)
   */
  public Set<String> getCollectionNames(String database) {
  try {
    // get the database
    DB db = getDB(database);
    Set<String> collections = db.getCollectionNames();
    logger.logp(Level.INFO, TAG, "getCollectionNames", " database:" + database);
    return collections;
  } catch (Exception e) {
    logger.logp(Level.SEVERE, TAG, "getCollectionNames", e.getMessage() + " database:" + database);
    e.printStackTrace();
  }
  return null;
  }

  /**
   * return a collection. 
   * Add the database to the dbMap if the first time we try to use it.
   * @param database
   * @param collectionName
   * @return db.getCollection(collectionName) (collection data)
   */
  // TODO: set as private
  public DBCollection getCollection(String database, String collectionName) {
    try {
      // get the database
      DB db = getDB(database);    
      // return the collection
      logger.logp(Level.INFO, TAG, "getCollection", " database:" + database + ", collection name:" + collectionName);
      return db.getCollection(collectionName);
    } catch (Exception e) {
      logger.logp(Level.SEVERE, TAG, "getCollection", e.getMessage() + " database:" + database + " collectionName:" + collectionName);
      e.printStackTrace();
    }
    return null;
  }
  
  /**
   * return a database
   * @param database
   * @return db 
   */
  //TODO: set as private
  public DB getDB(String database){
    try {
      DB db;
      // check if we already have this database in the Map
      if(dbMap.containsKey(database)){
        db = dbMap.get(database);
      } else {
        db = mongo.getDB(database);
        // add the database to the Map
        dbMap.put(database, db);      
      }
      logger.logp(Level.INFO, TAG, "getDB", " database:" + database);
      // return the database
      return db;
    } catch (Exception e) {
      logger.logp(Level.SEVERE, TAG, "getDB", e.getMessage() + " database:" + database);
      e.printStackTrace();
    }
    return null;
  }
}