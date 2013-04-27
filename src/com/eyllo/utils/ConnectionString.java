package com.eyllo.utils;

public final class ConnectionString {
	
	private static String strServer = "ec2-23-20-190-52.compute-1.amazonaws.com";
	private static int strPort = 27017;
	
	public static String getStrServer() {
		return strServer;
	}
	public static int getStrPort() {
		return strPort;
	}	
}
