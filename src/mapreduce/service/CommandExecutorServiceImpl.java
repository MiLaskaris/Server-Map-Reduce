package com.aueb.distributed.mapreduce.service;


import java.util.Iterator;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.aueb.distributed.mapreduce.ServerThread;
import com.aueb.distributed.mapreduce.db.Dao;
import com.aueb.distributed.mapreduce.enums.Command;
import com.aueb.distributed.mapreduce.enums.Status;
import com.aueb.distributed.mapreduce.queue.TaskQueue;
import com.aueb.distributed.util.Constant;

public class CommandExecutorServiceImpl extends Thread implements  Observer {

	private ServerThread[] threads;

	private int maxClientsCount;

	private ServerThread serverThread;
	
	private String name;
	
	/** service that implements map reduce */
	private MapReduceService mapReduceService = new MapReduceServiceImpl();
	
	/** service that keeps status of commands */
	private CommandStatusStore commandStatus = new CommandStatusStoreImpl();

	/** service that keeps the results of the commands */
	private CommandResultStore results = new CommandResultStoreImpl();
	
	/** queue for tasks to be executed */
	private TaskQueue taskQueue;
	
	
	
	/**
	 * Initialization of Task Queue
	 * @param taskQueue
	 */
    public CommandExecutorServiceImpl(TaskQueue taskQueue) {
		super();
		this.taskQueue = taskQueue;
	}



    /**
     * Execution of Commands
     * 
     */
    @Override
    public void run() {
        ConcurrentHashMap<String, Object> commandMap;
        while (true)
            if (!taskQueue.isQEmpty()) {
            	commandMap = taskQueue.getMessage();    	            	
            	execute(commandMap);
            } else
                synchronized (this) {
                    try {
                        wait();
                    } catch (final InterruptedException e) {  
                    	e.printStackTrace();
                    }
                }
    }
	
	public Status execute(ConcurrentHashMap<String, Object> commandMap) {

		/** command */
		Command command = (Command) commandMap.get(Constant.COMMAND);

		/** implementing logic for each different command of the system */
		if (Command.QUIT.equals(command)) {
			/** breaking while loop */
		}
		else if (Command.SEND_COMMAND_RETURN.equals(command))
		{
			/** sends back to requested the command return */
			this.serverThread.getOs().println((String) commandMap.get(Constant.RETURN_MESSAGE));
		}
		else if (Command.FRIEND_RECOMMENDATION_BY_EVENT_ATTENDANCE.equals(command)) {
			commandStatus.putCommandStatus((String)commandMap.get(Constant.REQUEST_ID),Status.STARTED );
			
			/** get variables from the map */
			Long memberId = Long.parseLong((String) commandMap.get(Constant.MEMBER_ID));
			Long memberNum = Long.parseLong((String) commandMap.get(Constant.MEMBERS_NUMBER));
			commandStatus.putCommandStatus((String)commandMap.get(Constant.REQUEST_ID),Status.IN_PROGRESS );		      			
			Set<Long> members = mapReduceService.getRecommendedFriendsByEventAttendance(memberId, memberNum);
			
			/** save result for future queries */
			results.putCommandResult((String)commandMap.get(Constant.REQUEST_ID), members);			
			commandStatus.putCommandStatus((String)commandMap.get(Constant.REQUEST_ID),Status.COMPLETED );
			System.out.println("[DEBUG] " +(String)commandMap.get(Constant.REQUEST_ID) + " Completed!");
		}
		else if (Command.EVENT_LIST_AROUND_MEMBER_BY_DISTANCE.equals(command)) {
			commandStatus.putCommandStatus((String)commandMap.get(Constant.REQUEST_ID),Status.STARTED );			
			/** get variables from the map */
			String memberId = (String) commandMap.get(Constant.MEMBER_ID);
			String distanceInKm = (String) commandMap.get(Constant.DISTANCE_IN_KM);
			String eventNumber = (String) commandMap.get(Constant.EVENTS_NUMBER);
			
			/** initialization */
			long memberIdL=0;
			long distanceInKmL=0;
			long eventNumL = 0;
		      try {
		          memberIdL = Long.parseLong(memberId);
		          distanceInKmL = Long.parseLong(distanceInKm);
		          eventNumL = Long.parseLong(eventNumber);
		       } catch (NumberFormatException nfe) {
		          System.out.println("[Exception] NumberFormatException: " + nfe.getMessage());
		       }						
			commandStatus.putCommandStatus((String)commandMap.get(Constant.REQUEST_ID),Status.IN_PROGRESS );		      
			/** long memberId, long distanceInKm */
			Set<String> events = mapReduceService.getEventIdAroundMember(memberIdL, distanceInKmL, eventNumL);				
			results.putCommandResult((String)commandMap.get(Constant.REQUEST_ID), events);	
			commandStatus.putCommandStatus((String)commandMap.get(Constant.REQUEST_ID),Status.COMPLETED );		
			System.out.println("[DEBUG] " +(String)commandMap.get(Constant.REQUEST_ID) + " Completed!");
		}
		else if (Command.SEND_PRIVATE_MESSAGE.equals(command)) {
			String line = (String) commandMap.get(Constant.MESSAGE);			
			String[] words = line.split("\\s", 2);
			if (words.length > 1 && words[1] != null) {
				words[1] = words[1].trim();
				if (!words[1].isEmpty()) {
					synchronized (this) {
						for (int i = 0; i < maxClientsCount; i++) {
							if (threads[i] != null
									&& threads[i] != serverThread
									&& threads[i].getClientName() != null
									&& threads[i].getClientName().equals(
											words[0])) {
								threads[i].getOs().println(
										this.name + ":"
												+ words[1]);

								System.out.println("Private msg from: "
										+ this.name + " to "
										+ threads[i].getClientName() + " msg: "
										+ words[1]);
								break;
							}
						}
					}
				}
			}
			commandStatus.putCommandStatus((String)commandMap.get(Constant.REQUEST_ID),Status.COMPLETED );
			System.out.println("[DEBUG] " +(String)commandMap.get(Constant.REQUEST_ID) + " Completed!");
			return Status.COMPLETED;
		}
		else if (Command.SEND_PUBLIC_MESSAGE.equals(command)) {
			String line = (String) commandMap.get(Constant.MESSAGE);
			/** sending public messages */
			synchronized (this) {
				for (int i = 0; i < maxClientsCount; i++) {
					if (threads[i] != null
							&& threads[i].getClientName() != null) {
						threads[i].getOs().println(
								this.name + ": " + line);
					}
				}
				System.out.println("<" + this.name + "> " + line);
			}
			commandStatus.putCommandStatus((String)commandMap.get(Constant.REQUEST_ID),Status.COMPLETED );
			System.out.println("[DEBUG] " + (String)commandMap.get(Constant.REQUEST_ID) + " Completed!");
		}
		else if (Command.REQUEST_STATUS_BY_REQUEST_ID.equals(command)) {
			String requestId = (String) commandMap.get(Constant.REQUEST_ID);
			Status status  = commandStatus.getCommandStatus(requestId);
			if (status != null) {
				this.serverThread.getOs().println(requestId + " status:" + status.toString());
			} else {				
				this.serverThread.getOs().println(requestId + " status:" + " non Existing Request " );				
			}
			commandStatus.putCommandStatus((String)commandMap.get(Constant.REQUEST_ID),Status.COMPLETED );
			System.out.println("[DEBUG] " +(String)commandMap.get(Constant.REQUEST_ID) + " Completed!");
		}
		else if (Command.REQUEST_RESULT_BY_REQUEST_ID.equals(command)) {
			String requestId = (String) commandMap.get(Constant.REQUEST_ID);
			Set<? extends Object> ids = results.getCommandResult(requestId);
			if (ids != null) {
				Iterator<? extends Object> iter = ids.iterator();
				this.serverThread.getOs().println(requestId + " results:");
				while (iter.hasNext()) {
					this.serverThread.getOs().println( iter.next().toString());
				}
			} else {
				this.serverThread.getOs().println(
						requestId + " Non Existing result");
			}
			commandStatus.putCommandStatus(
					(String) commandMap.get(Constant.REQUEST_ID),
					Status.COMPLETED);
			System.out.println("[DEBUG] " +(String)commandMap.get(Constant.REQUEST_ID) + " Completed!");
		}
		else if (Command.EVENT_VOTING.equals(command)) {
			String requestId = (String) commandMap.get(Constant.REQUEST_ID);
			Long memberId = Long.parseLong((String) commandMap.get(Constant.MEMBER_ID));
			Long eventId = Long.parseLong((String) commandMap.get(Constant.EVENT_ID));
			String vote = ((String) commandMap.get(Constant.VOTE));
			Dao.getInstance().setMembersEventsAttendance(memberId, eventId, vote);
			commandStatus.putCommandStatus(requestId, Status.COMPLETED );
			System.out.println("[DEBUG] " +(String)commandMap.get(Constant.REQUEST_ID) + " Completed!");
		}
		else if (Command.BEFRIEND.equals(command)) {
			String requestId = (String) commandMap.get(Constant.REQUEST_ID);
			Long memberId1 = Long.parseLong((String) commandMap.get(Constant.MEMBER_ID1));
			Long memberId2 = Long.parseLong((String) commandMap.get(Constant.MEMBER_ID2));						
			/**  we need to check which order is in db */			
			Dao.getInstance().setMembersRelationship(memberId1, memberId2, "FRIENDS");			
			commandStatus.putCommandStatus(requestId, Status.COMPLETED );
			System.out.println("[DEBUG] " +(String)commandMap.get(Constant.REQUEST_ID) + " Completed!");
		}
		else if (Command.UNFRIEND.equals(command)) {
			String requestId = (String) commandMap.get(Constant.REQUEST_ID);
			Long memberId1 = Long.parseLong((String) commandMap.get(Constant.MEMBER_ID1));
			Long memberId2 = Long.parseLong((String) commandMap.get(Constant.MEMBER_ID2));				
			/**  we need to check which order is in db */									
			Dao.getInstance().setMembersRelationship(memberId1, memberId2,"ACQUAINTANCE");									
			commandStatus.putCommandStatus(requestId, Status.COMPLETED );
			System.out.println("[DEBUG] " +(String)commandMap.get(Constant.REQUEST_ID) + " Completed!");
		}
		else if(Command.FRIEND_LIST.equals(command))
		{
			String requestId = (String) commandMap.get(Constant.REQUEST_ID);
			Long memberId = Long.parseLong((String) commandMap
					.get(Constant.MEMBER_ID));
			Set<Long> relationships = Dao.getInstance()
					.getRelationshipsForMember(memberId, "FRIENDS");

			if (relationships != null) {
				Set<String> readableResult = Dao.getInstance()
						.getReadableFriends(relationships);
				Iterator<String> iter = readableResult.iterator();
				this.serverThread.getOs().println(requestId + " results:");
				while (iter.hasNext()) {
					this.serverThread.getOs().println(iter.next());
				}
			} else {
				this.serverThread.getOs().println(
						requestId + " Non Existing result");
			}
			commandStatus.putCommandStatus(
					(String) commandMap.get(Constant.REQUEST_ID),
					Status.COMPLETED);
			System.out.println("[DEBUG] " +(String)commandMap.get(Constant.REQUEST_ID) + " Completed!");
		}
		else if(Command.EVENT_LIST.equals(command))
		{
			String requestId = (String) commandMap.get(Constant.REQUEST_ID);
			Long memberId = Long.parseLong((String) commandMap
					.get(Constant.MEMBER_ID));
			Set<String> eventIds = Dao.getInstance().getEventsAttended(memberId);

			if (eventIds != null) {
				Set<String> readableResult = Dao.getInstance()
						.getReadableEvents(eventIds);
				Iterator<String> iter = readableResult.iterator();
				this.serverThread.getOs().println(requestId + " results:");
				while (iter.hasNext()) {
					this.serverThread.getOs().println(iter.next());
				}
			} else {
				this.serverThread.getOs().println(
						requestId + " Non Existing result");
			}
			commandStatus.putCommandStatus(
					(String) commandMap.get(Constant.REQUEST_ID),
					Status.COMPLETED);
			System.out.println("[DEBUG] " +(String)commandMap.get(Constant.REQUEST_ID) + " Completed!");
		}
				
		return Status.COMPLETED;
	}

	public void setMaxClientsCount(int maxClientsCount) {
		this.maxClientsCount = maxClientsCount;
	}

	public void setThreads(ServerThread[] threads) {
		this.threads = threads;
	}
	
	public void setServerThread(ServerThread serverThread) {
		this.serverThread = serverThread;
	}

	public void setThisName(String name) {
		this.name = name;		
	}

	@Override
	public void update(Observable o, Object arg) {
        synchronized (this) {
            notify();
        }		
	}
	
}
