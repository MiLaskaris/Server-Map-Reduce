package com.aueb.distributed.mapreduce.service;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CommandResultStoreImpl implements CommandResultStore {

	ConcurrentHashMap<String, Set<? extends Object>> commandResult = new ConcurrentHashMap<>();
	ConcurrentHashMap<String, Set<? extends Object>> commandResultShortId = new ConcurrentHashMap<>();

	@Override
	public Set<? extends Object> getCommandResult(String requestId) {
		if (requestId.length() == 3)
			return commandResultShortId.get(requestId.trim());
		else
			return commandResult.get(requestId);
	}

	@Override
	public void putCommandResult(String requestId, Set<? extends Object> Ids) {
		commandResult.put(requestId, Ids);
		commandResultShortId.put(requestId.substring(0, 3).trim(), Ids);
	}

}
