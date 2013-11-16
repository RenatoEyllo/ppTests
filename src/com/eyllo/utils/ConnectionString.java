package com.eyllo.utils;

public final class ConnectionString {
	
	private static String strServer = "mongodb-us-e.cloudapp.net";
	private static int strPort = 27017;
	
	public static String getStrServer() {
		return strServer;
	}
	public static int getStrPort() {
		return strPort;
	}	
}
