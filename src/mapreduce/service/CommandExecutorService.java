package com.aueb.distributed.mapreduce.service;

import java.util.concurrent.ConcurrentHashMap;

import com.aueb.distributed.mapreduce.ServerThread;
import com.aueb.distributed.mapreduce.enums.Status;

public interface CommandExecutorService   {
	
	public Status execute(ConcurrentHashMap<String, Object> commandMap);
	
	public void setMaxClientsCount(int maxClientsCount);
	
	public void setThreads(ServerThread[] threads);
	
	public void setServerThread(ServerThread serverThread);
	
	public void setThisName(String name);

}
