package com.aueb.distributed.mapreduce.queue;

import java.util.Collections;
import java.util.Observable;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;


public class TaskQueue extends Observable {

	private static Vector<ConcurrentHashMap<String, Object>> tasks;

	public static synchronized Vector<ConcurrentHashMap<String, Object>> getInstance() {
		if (tasks == null) {
			tasks = new Vector<ConcurrentHashMap<String, Object>>();
		}
		return tasks;
	}

	public synchronized void addMessage(
			final ConcurrentHashMap<String, Object> commandMap) {
		tasks.add(commandMap);
		setChanged();
		notifyObservers();
	}

	
    public synchronized ConcurrentHashMap<String, Object> getMessage() {
        if (!tasks.isEmpty())
            return tasks.remove(0);
        return null;
    }
    
	public synchronized void clear() {
		tasks.clear();
	}

	public synchronized ConcurrentHashMap<String, Object> getTask() {
		if (!tasks.isEmpty())
			return tasks.remove(0);
		return null;
	}

	public synchronized int getSize() {
		return tasks.size();
	}

	public synchronized boolean isQEmpty() {
		return tasks.isEmpty();
	}

	public synchronized void shuffle() {
		Collections.shuffle(tasks);
	}

	public synchronized void trimSize() {
		tasks.trimToSize();
	}
	
	public synchronized Vector<ConcurrentHashMap<String, Object>> getTasksQueue() {
		return tasks;		
	}
}