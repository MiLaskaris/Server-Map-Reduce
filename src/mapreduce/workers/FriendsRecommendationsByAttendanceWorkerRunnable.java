package com.aueb.distributed.mapreduce.workers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import com.aueb.distributed.mapreduce.db.Dao;


/**
 * Thread runnable that implements the friend recommendation based on the attandance preference
 * on events.
 *
 */
public class FriendsRecommendationsByAttendanceWorkerRunnable implements Runnable {

	/** job id for worker execution */
	private int jobId;
	
	/** member id*/
	private long memberId;
	
	/** subset of events */
	private Set<String> events;
	
	private long index;
	
	private long numEvents;
		
	/** reference to result aggregator */
	private ConcurrentHashMap< Integer, Map<Long, Long>> resultsAggregator;
	
	/** reference to active workers */
	private ConcurrentHashMap< Integer, Integer> activeWorkers;
	
	private Set<Long> resultIds =  new HashSet<>();
	
	
	/**
	 * Constructor.
	 * 
	 * @param memberId
	 * @param index
	 * @param entries
	 * @param resultsAggregator
	 * @param activeWorkers
	 */
	public FriendsRecommendationsByAttendanceWorkerRunnable(int jobId, long memberId, long index, long numEvents ,
			ConcurrentHashMap<Integer, Map<Long, Long>> resultsAggregator, ConcurrentHashMap<Integer, Integer> activeWorkers) {
		super();
		this.jobId = jobId;
		this.memberId = memberId;
		this.index = index;
		this.numEvents = numEvents;
		this.resultsAggregator = resultsAggregator;
		this.activeWorkers = activeWorkers;
	}


	
	/**
	 * Procedure that each worker executes
	 */
	@Override
	public void run() {
		
		Map<Long, Long> membersAttendance = new HashMap<>();		
		Set<String> events  = Dao.getInstance().getPaginatedAttendedEventsId(memberId, index, numEvents);
		System.out.println("[DEBUG] [Map Reduce Worker - " + jobId + "] Calculating Recommended Friends based on: "+ events.size() + " events: " + events.toString());
		/** Get Map of Result */
		if (events.size() > 0)
		{
			membersAttendance = Dao.getInstance().getMembersAttendanceOfEvent(events);			
		}

		
		/** save completed job to results aggregator */
		resultsAggregator.put(getJobId(), membersAttendance);

		/** */
		activeWorkers.remove(getJobId());
	}


	public int getJobId() {
		return jobId;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	public long getMemberId() {
		return memberId;
	}

	public void setMemberId(long memberId) {
		this.memberId = memberId;
	}

	public ConcurrentHashMap<Integer, Map<Long, Long>> getResultsAggregator() {
		return resultsAggregator;
	}

	public void setResultsAggregator(
			ConcurrentHashMap<Integer, Map<Long, Long>> resultsAggregator) {
		this.resultsAggregator = resultsAggregator;
	}

	public ConcurrentHashMap<Integer, Integer> getActiveWorkers() {
		return activeWorkers;
	}

	public void setActiveWorkers(ConcurrentHashMap<Integer, Integer> activeWorkers) {
		this.activeWorkers = activeWorkers;
	}

	public Set<Long> getEventIds() {
		return resultIds;
	}

	public void setEventIds(Set<Long> eventIds) {
		this.resultIds = eventIds;
	}
}
