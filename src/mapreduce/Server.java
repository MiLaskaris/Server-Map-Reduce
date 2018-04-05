package com.aueb.distributed.mapreduce;

import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * 
 * Server class
 * 
 */
public class Server {

	private static ServerSocket serverSocket = null;

	private static Socket clientSocket = null;

	private static final int MAX_CLIENT_COUNT = 10;

	private static final ServerThread[] threads = new ServerThread[MAX_CLIENT_COUNT];

	private static final int PORT_NUMBER = 2222;

	public void start(String args[]) {

		if (args.length < 1) {
			System.out.println("[DEBUG] Chat server started at port: " + PORT_NUMBER);
		}

		/** starting map reduce server */
		serverSocket = startServer(PORT_NUMBER);

		/** accepting new clients */
		initializeNewServerThreads();

	}

	/**
	 * Initialize new client serving threads or rejecting new threads creation
	 * when reached the threshold.
	 * 
	 */
	private void initializeNewServerThreads() {
		while (true) {
			try {
				clientSocket = serverSocket.accept();
				int i = 0;
				for (i = 0; i < MAX_CLIENT_COUNT; i++) {
					if (threads[i] == null) {
						(threads[i] = new ServerThread(clientSocket, threads))
								.start();
						break;
					}
				}
				if (i == MAX_CLIENT_COUNT) {
					PrintStream os = new PrintStream(
							clientSocket.getOutputStream());
					os.println("Server too busy. Try later.");
					os.close();
					clientSocket.close();
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

	}

	/**
	 * Start a server socket on a port defined.
	 * 
	 * @param portNumber
	 * @return server socket
	 */
	private ServerSocket startServer(int portNumber) {
		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(portNumber);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return serverSocket;
	}

}
