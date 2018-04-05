package com.aueb.distributed.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.Socket;
import java.util.Observer;
import java.util.concurrent.ConcurrentHashMap;

import com.aueb.distributed.mapreduce.queue.TaskQueue;
import com.aueb.distributed.mapreduce.service.CommandExecutorService;
import com.aueb.distributed.mapreduce.service.CommandExecutorServiceImpl;
import com.aueb.distributed.mapreduce.service.ParseCommandService;
import com.aueb.distributed.mapreduce.service.ParseCommandServiceImpl;

/**
 * Server Thread class
 */
public class ServerThread extends Thread {
	private String clientName = null;

	private BufferedReader is = null;
	private PrintStream os = null;
	private Socket clientSocket = null;
	private final ServerThread[] threads;
	private int maxClientsCount;
	@SuppressWarnings("unused")
	private String name = null;
	
	TaskQueue taskQueue = new TaskQueue();



	/** parse command service */
	private ParseCommandService parseCommandService = new ParseCommandServiceImpl(taskQueue);

	/** command executor */
	private CommandExecutorServiceImpl commandExecutor = new CommandExecutorServiceImpl(taskQueue);
	

	/**
	 * Server Thread constructor.
	 * 
	 * @param clientSocket
	 * @param threads
	 */
	public ServerThread(Socket clientSocket, ServerThread[] threads) {
		this.clientSocket = clientSocket;
		this.threads = threads;
		maxClientsCount = threads.length;
	}

	/**
	 * The server running thread...
	 */
	public void run() {
		int maxClientsCount = this.maxClientsCount;
		ServerThread[] threads = this.threads;

		try {
			/** initialization of threads */
			commandExecutor.start();
			taskQueue.getInstance();
			taskQueue.addObserver(commandExecutor);
			
			/** receives messages from the client */
			is = new BufferedReader(new InputStreamReader(
					clientSocket.getInputStream()));
			/** send messages to the client **/
			os = new PrintStream(clientSocket.getOutputStream());

			/** 1. initialization of server thread to serve the client */
			String name = initializeThread(is, os);

			/** 2. client exchanges messages with other mobile users */
			whileAtClientMessageParser(name, is, os);

			/** 3. client is exiting */
			stopingThread(name, is, os);

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				is.close();
				os.close();
				clientSocket.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	/**
	 * Stopping the server thread message.
	 * 
	 * @param name
	 * @param is
	 * @param os
	 * @throws IOException
	 */
	private void stopingThread(String name, BufferedReader is, PrintStream os)
			throws IOException {
		synchronized (this) {
			for (int i = 0; i < maxClientsCount; i++) {
				if (threads[i] != null && threads[i] != this
						&& threads[i].clientName != null) {
					threads[i].os.println(name + " has left \n");

				}
			}
		}
		os.println(" Bye " + name + "\n");
		System.out.println("[DEBUG] " + this.clientName.substring(1)
				+ " has left the chatroom \n");

		synchronized (this) {
			for (int i = 0; i < maxClientsCount; i++) {
				if (threads[i] == this) {
					threads[i] = null;
				}
			}
		}
		is.close();
		os.close();
		clientSocket.close();
	}

	/**
	 * Parse mobile messages, send private or public messages.
	 * 
	 * @param name
	 * @param is
	 * @param os
	 * @throws IOException
	 */
	private void whileAtClientMessageParser(String name, BufferedReader is,
			PrintStream os) throws IOException {

		/** initialization of executor */
		commandExecutor.setMaxClientsCount(maxClientsCount);
		commandExecutor.setServerThread(this);
		commandExecutor.setThreads(threads);
		commandExecutor.setThisName(name);
		
		while (true) {			
			
			String line = null;			
			try {
				line = is.readLine();
			} catch (Exception ex) {
				ex.printStackTrace();
				break;
			}			
			if (line != null) {
				/** client exiting /quit */
				if (line.startsWith("/quit")) {
					break;
				} 
				parseCommandService.execute(line);
			}
		}
	}

	/**
	 * Get client name and greet.
	 * 
	 * @param is
	 * @param os
	 * @return
	 * @throws IOException
	 */
	private String initializeThread(BufferedReader is, PrintStream os)
			throws IOException {
		String name = "";
		while (true) {
			os.println("Enter your name.");
			name = is.readLine().trim();
			if (name.indexOf('@') == -1) {
				break;
			} else {
				os.println("The name should not contain '@' character.");
			}
		}
		os.println("************************** \n"
				+ "Welcome "
				+ name
				+ " to our chat room.\nTo leave enter /quit in a new line.\n To private message use @Username\n **************************");
		
		this.name = name;
		synchronized (this) {
			for (int i = 0; i < maxClientsCount; i++) {
				if (threads[i] != null && threads[i] == this) {
					clientName = "@" + name;
					break;
				}
			}
			for (int i = 0; i < maxClientsCount; i++) {
				if (threads[i] != null && threads[i] != this) {
					threads[i].os.println(name
							+ " has entered the chat room !!!");
				}
			}
			System.out.println("[DEBUG] " + this.clientName.substring(1)
					+ " has entered the chat room !!! ");
		}
		return name;
	}

	public PrintStream getOs() {
		return os;
	}

	public void setOs(PrintStream os) {
		this.os = os;
	}

	public String getClientName() {
		return clientName;
	}

	public void setClientName(String clientName) {
		this.clientName = clientName;
	}

	public Socket getClientSocket() {
		return clientSocket;
	}

	public void setClientSocket(Socket clientSocket) {
		this.clientSocket = clientSocket;
	}

	public int getMaxClientsCount() {
		return maxClientsCount;
	}

	public void setMaxClientsCount(int maxClientsCount) {
		this.maxClientsCount = maxClientsCount;
	}

}
