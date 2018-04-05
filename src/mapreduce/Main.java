package com.aueb.distributed.mapreduce;

public class Main {

	public static void main(String args[]) {
		/** get the arguments from command line */
		String[] arguments = new String[] {};
		/** initialize a new map reduce server */
		Server server = new Server();
		/** start the map reduce server */
		server.start(arguments);
	}

}
