package com.aueb.distributed.mapreduce.service;

import com.aueb.distributed.mapreduce.enums.Status;

public interface CommandStatusStore {
	
	public Status getCommandStatus(String requestId);
	
	public void putCommandStatus(String requestId, Status status);
}
