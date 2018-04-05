package com.aueb.distributed.mapreduce.service;

import java.util.Set;

public interface CommandResultStore {

	public Set<? extends Object> getCommandResult(String requestId);
	
	public void putCommandResult(String requestId, Set<? extends Object> Ids);

}
