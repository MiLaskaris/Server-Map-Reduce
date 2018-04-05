package com.aueb.distributed.mapreduce.service;

import java.util.concurrent.ConcurrentHashMap;

import com.aueb.distributed.mapreduce.enums.Command;
import com.aueb.distributed.mapreduce.queue.TaskQueue;
import com.aueb.distributed.util.Constant;


/**
 
Class that parses the API calls described below:

  
get friends recommendation for member [cmd] /getFriends,[member_id],[friends_number]
  
get events recommendation for member  [cmd] /getEvents,[member_id],[distance],[events_number]
 
vote for attending an event           [cmd] /goEvent,[member_id],[event_id],[vote]

member_1 befriends member 2           [cmd] /beFriend,[member_id_1],[member_id_2]

member_1 unfriends member_2 	      [cmd] /unFriend,[member_id_1],[member_id_2]

get friends for member                [cmd] /getFriendsList,[member_id]

get events for member 				  [cmd] /getEventsList,[member_id]

get status of request				  [cmd] /status,[request_id]

get the result of a request			  [cmd] /result,[request_id]

personal message 					  [cmd] @[member_id] [msg]

public message 						  [cmd] [msg]

details for member                    [cmd] /getMember,[member_id]

quit the chat                         [cmd] /quit

*
*/


public class ParseCommandServiceImpl implements ParseCommandService {
	
	
	public ParseCommandServiceImpl(TaskQueue taskQueue) {
		super();
		this.taskQueue = taskQueue;
	}

	TaskQueue taskQueue;

	@Override
	public void execute(String line) {
		ConcurrentHashMap<String, Object> commandMap = new ConcurrentHashMap<String, Object>();

		/** service that differentiates the service */
		RequestDifferentiatorService requestDifferentiator = new RequestDifferentiatorSeviceImp();

		if (line.startsWith("/quit") || line.startsWith("/q")) {
			commandMap.put(Constant.COMMAND, Command.QUIT);
			commandMap.put(Constant.REQUEST_ID,
					requestDifferentiator.getRequestID());
		}
		else if (line.startsWith("/status") || line.startsWith("/s")) {
			commandMap.put(Constant.COMMAND,
					Command.REQUEST_STATUS_BY_REQUEST_ID);
			String[] tokens = line.split(",");
			if (tokens.length == 2) {
				commandMap.put(Constant.REQUEST_ID, tokens[1]);
				System.out.println("[DEBUG] "+line + " : with request id: "
						+ (String) commandMap.get(Constant.REQUEST_ID));
			} else {
				System.out
						.println("[DEBUG] The command format is: /status,[request_id]");
				commandMap.clear();
				commandMap.put(Constant.COMMAND,
						Command.SEND_COMMAND_RETURN);
				commandMap.put(Constant.RETURN_MESSAGE,
						"The command format is: /status,[request_id]");
			}
		}
		else if (line.startsWith("/getMember") || line.startsWith("/gm")) {
			commandMap.put(Constant.COMMAND, Command.MEMBER_INFORMATION);
			commandMap.put(Constant.REQUEST_ID,
					requestDifferentiator.getRequestID());
			
			String[] tokens = line.split(",");
			/** if all the variables are set */
			if (tokens.length == 2) {
				commandMap.put(Constant.MEMBER_ID, tokens[1]);
			} else {
				System.out
						.println("[DEBUG] "+ "The command format is: /getMember,[member_id]");
				commandMap.clear();
				commandMap.put(Constant.COMMAND,
						Command.SEND_COMMAND_RETURN);
				commandMap.put(Constant.RETURN_MESSAGE,
						"The command format is: /getMember,[member_id]");
			}			
		}
		else if (line.startsWith("/result") || line.startsWith("/r")) {
			commandMap.put(Constant.COMMAND,
					Command.REQUEST_RESULT_BY_REQUEST_ID);
			commandMap.put(Constant.REQUEST_ID,
					requestDifferentiator.getRequestID());
			
			String[] tokens = line.split(",");
			if (tokens.length == 2) {
				commandMap.put(Constant.REQUEST_ID, tokens[1]);
				System.out.println("[DEBUG] "+ line + " : with request id: "
						+ (String) commandMap.get(Constant.REQUEST_ID));
			} else {
				System.out
						.println("[DEBUG] "+"The command format is: /getMember,[member_id]");
				commandMap.clear();
				commandMap.put(Constant.COMMAND,
						Command.SEND_COMMAND_RETURN);
				commandMap.put(Constant.RETURN_MESSAGE,
						"The command format is: /getMember,[member_id]");
			}
		}
		else if (line.startsWith("/getFriends") || line.startsWith("/gf")) {
			commandMap.put(Constant.COMMAND,
					Command.FRIEND_RECOMMENDATION_BY_EVENT_ATTENDANCE);
			commandMap.put(Constant.REQUEST_ID,
					requestDifferentiator.getRequestID());
			sendRequestIdBack(line, (String) commandMap.get(Constant.REQUEST_ID));
			
			String[] tokens = line.split(",");
			if (tokens.length == 3) {
				commandMap.put(Constant.MEMBER_ID, tokens[1]);
				commandMap.put(Constant.MEMBERS_NUMBER, tokens[2]);
				System.out.println("[DEBUG] "+ line + " : with request id: "
						+ (String) commandMap.get(Constant.REQUEST_ID));
			} else if (tokens.length == 2) {
				commandMap.put(Constant.MEMBER_ID, tokens[1]);
				System.out.println("[DEBUG] "+ line + " : with request id: "
						+ (String) commandMap.get(Constant.REQUEST_ID));
			} else {
				System.out
						.println("[DEBUG] The command format is: /getFriends,[member_id],[members_number]");
				commandMap.clear();
				commandMap.put(Constant.COMMAND,
						Command.SEND_COMMAND_RETURN);
				commandMap
						.put(Constant.RETURN_MESSAGE,
								"The command format is: /getFriends,[member_id],[members_number]");
			}
		}				
		else if (line.startsWith("/getEvents") || line.startsWith("/ge")) {
			commandMap.put(Constant.COMMAND,
					Command.EVENT_LIST_AROUND_MEMBER_BY_DISTANCE);
			String[] tokens = line.split(",");
			if (tokens.length == 4) {
				commandMap.put(Constant.MEMBER_ID, tokens[1]);
				commandMap.put(Constant.DISTANCE_IN_KM, tokens[2]);
				commandMap.put(Constant.EVENTS_NUMBER, tokens[3]);
				commandMap.put(Constant.REQUEST_ID,
						requestDifferentiator.getRequestID());
				System.out.println("[DEBUG] " + line + " : with request id: "
						+ (String) commandMap.get(Constant.REQUEST_ID));
			} else {
				System.out
						.println("[DEBUG] The command format is: /getEvents,[member_id],[distance_in_km],[events_num]");
				commandMap.clear();
				commandMap.put(Constant.COMMAND, Command.SEND_COMMAND_RETURN);
				commandMap
						.put(Constant.RETURN_MESSAGE,
								"The command format is: /getEvents,[member_id],[distance_in_km],[events_num]");
			}
		}
		else if (line.startsWith("/befriend") || line.startsWith("/bf"))
		{			
			commandMap.put(Constant.COMMAND, Command.BEFRIEND);
			commandMap.put(Constant.REQUEST_ID,
					requestDifferentiator.getRequestID());
						
			String[] tokens = line.split(",");
			if (tokens.length == 3) {
				commandMap.put(Constant.MEMBER_ID1, tokens[1]);
				commandMap.put(Constant.MEMBER_ID2, tokens[2]);
				System.out.println("[DEBUG] "+ line + " : with request id: "
						+ (String) commandMap.get(Constant.REQUEST_ID));
			} else {
				System.out
						.println("[DEBUG] The command format is: /beFriend,[member_id1],[member_id2]");
				commandMap.clear();
				commandMap.put(Constant.COMMAND,
						Command.SEND_COMMAND_RETURN);
				commandMap
						.put(Constant.RETURN_MESSAGE,
								"The command format is: /beFriend,[member_id1],[member_id2]");
			}
		}
		else if (line.startsWith("/unfriend") || line.startsWith("/uf"))
		{			
			commandMap.put(Constant.COMMAND, Command.UNFRIEND);
			commandMap.put(Constant.REQUEST_ID,
					requestDifferentiator.getRequestID());
			
			String[] tokens = line.split(",");
			if (tokens.length == 3) {
				commandMap.put(Constant.MEMBER_ID1, tokens[1]);
				commandMap.put(Constant.MEMBER_ID2, tokens[2]);
				System.out.println("[DEBUG] "+line + " : with request id: "
						+ (String) commandMap.get(Constant.REQUEST_ID));
			} else {
				System.out
						.println("[DEBUG] The command format is: /unFriend,[member_id1],[member_id2]");
				commandMap.clear();
				commandMap.put(Constant.COMMAND,
						Command.SEND_COMMAND_RETURN);
				commandMap
						.put(Constant.RETURN_MESSAGE,
								"The command format is: /unFriend,[member_id1],[member_id2]");
			}
		}		
		else if (line.startsWith("/vote") || line.startsWith("/v"))
		{			
			commandMap.put(Constant.COMMAND, Command.EVENT_VOTING);
			commandMap.put(Constant.REQUEST_ID, requestDifferentiator.getRequestID());
						
			String[] tokens = line.split(",");
			if (tokens.length == 4)
			{
			commandMap.put(Constant.MEMBER_ID, tokens[1]);
			commandMap.put(Constant.EVENT_ID, tokens[2]);
			commandMap.put(Constant.VOTE, tokens[3]);			
			System.out.println("[DEBUG] "+line + " : with request id: "+ (String) commandMap.get(Constant.REQUEST_ID));
			}
			else
			{
				System.out
				.println("[DEBUG] The command format is: /vote,[member_id],[event_id],[attendance]");
		commandMap.clear();
		commandMap.put(Constant.COMMAND,
				Command.SEND_COMMAND_RETURN);
		commandMap
				.put(Constant.RETURN_MESSAGE,
						"The command format is: /vote,[member_id],[event_id],[attendance]");				
			}
		}
		else if (line.startsWith("/listFriends") || line.startsWith("/lf"))
		{
			commandMap.put(Constant.COMMAND, Command.FRIEND_LIST);
			commandMap.put(Constant.REQUEST_ID,
					requestDifferentiator.getRequestID());
			String[] tokens = line.split(",");
			if (tokens.length == 2) {
				commandMap.put(Constant.MEMBER_ID, tokens[1]);
				System.out.println("[DEBUG] "+line + " : with request id: "
						+ (String) commandMap.get(Constant.REQUEST_ID));
			} else {
				commandMap.clear();
				commandMap.put(Constant.COMMAND, Command.SEND_COMMAND_RETURN);
				commandMap.put(Constant.RETURN_MESSAGE,
						"The command format is: /getFriendList,[member_id]");

			}
		}
		else if (line.startsWith("/listEvents") || line.startsWith("/le"))
		{
			commandMap.put(Constant.COMMAND, Command.EVENT_LIST);
			commandMap.put(Constant.REQUEST_ID,
					requestDifferentiator.getRequestID());
			String[] tokens = line.split(",");
			if (tokens.length == 2) {
				commandMap.put(Constant.MEMBER_ID, tokens[1]);
				System.out.println("[DEBUG] "+ line + " : with request id: "
						+ (String) commandMap.get(Constant.REQUEST_ID));
			} else {
				commandMap.clear();
				commandMap.put(Constant.COMMAND, Command.SEND_COMMAND_RETURN);
				commandMap.put(Constant.RETURN_MESSAGE,
						"The command format is: /listEvents,[member_id]");

			}
		}		
		else if (line.startsWith("@")) {
			commandMap.put(Constant.COMMAND, Command.SEND_PRIVATE_MESSAGE);
			commandMap.put(Constant.REQUEST_ID,
					requestDifferentiator.getRequestID());
			commandMap.put(Constant.MESSAGE, line);
			System.out.println("[DEBUG] Private message Command: " +line +  " : with request Id: "+ (String) commandMap.get(Constant.REQUEST_ID));
		}
		else {
			commandMap.put(Constant.COMMAND, Command.SEND_PUBLIC_MESSAGE);
			commandMap.put(Constant.REQUEST_ID,
					requestDifferentiator.getRequestID());
			
			commandMap.put(Constant.MESSAGE, line);
			System.out.println("[DEBUG] Public message Command: " +line +  " : with request Id: "+ (String) commandMap.get(Constant.REQUEST_ID));
		}
		
		taskQueue.addMessage(commandMap);
	}

	/**
	 * 
	 * @param line
	 * @param requestId
	 */
	private void sendRequestIdBack(String line, String requestId) {
		ConcurrentHashMap<String, Object> returnCommandMap = new ConcurrentHashMap<String, Object>();
		returnCommandMap.put(Constant.COMMAND,
				Command.SEND_COMMAND_RETURN);
		returnCommandMap.put(Constant.RETURN_MESSAGE, "command:" + line + " requestId:" +(String) requestId);
		taskQueue.addMessage(returnCommandMap);
		
	}
}
