/**
 * PRIVATE AND CONFIDENTIAL - Copyright (C) 2012 - eyllo tecnologia  
 *           www.eyllo.com
 *           
 * Java source file 
 * 
 * 
 * Abstracts access to MongoDB.
 * Implements a n-tier architecture
 * 
 * @author enylton
 *
 */

package com.eyllo.database;

import java.util.HashMap;

import com.eyllo.utils.ConnectionString;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class DataTierMongoDB {

  private MongoClient mongo;			// a MongoDB connection
  private HashMap<String, DB> dbMap;	// a Map with a list of databases

  public DataTierMongoDB() throws UnavailableException {
    try {
      mongo = new MongoClient(ConnectionString.getStrServer(), ConnectionString.getStrPort());
      dbMap = new HashMap<String, DB>();
    } catch (java.net.UnknownHostException e){
      e.printStackTrace();
      throw new UnavailableException("Could not open a connection to the database", 20);
    }
  }

	/**
	 * return a collection. 
	 * Add the database to the dbMap if the first time we try to use it.
	 * @param database
	 * @param collectionName
	 * @return
	 */
	public DBCollection returnCollection(String database, String collectionName) {
		DB db;
		// check if we already have this database in the Map
		if(dbMap.containsKey(database)){
			db = dbMap.get(database); 
		} else {
			db = mongo.getDB(database);
			// add the database to the Map
			dbMap.put(database, db);			
		}
		
		// return the collection
		return db.getCollection(collectionName);		
	}

}