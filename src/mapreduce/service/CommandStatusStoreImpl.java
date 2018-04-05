package com.aueb.distributed.mapreduce.service;

import java.util.concurrent.ConcurrentHashMap;

import com.aueb.distributed.mapreduce.enums.Status;

public class CommandStatusStoreImpl implements CommandStatusStore{

	ConcurrentHashMap< String, Status> commandStatus= new ConcurrentHashMap<>();
	ConcurrentHashMap< String, Status> commandStatusShortId= new ConcurrentHashMap<>();
	
	@Override
	public Status getCommandStatus(String requestId) {		
		if (requestId.length() == 3)
			return commandStatusShortId.get(requestId.trim());
		else
			return commandStatus.get(requestId);
	}

	@Override
	public void putCommandStatus(String requestId, Status status) {
		commandStatus.put(requestId, status);
		commandStatusShortId.put(requestId.substring(0, 3).trim(), status);
	}
}

