package com.aueb.distributed.mapreduce.workers;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.aueb.distributed.mapreduce.db.Dao;
import com.aueb.distributed.mapreduce.dto.Event;
import com.aueb.distributed.util.Utils;

public class EventsRecommendationByDistanceWorkerRunnable implements Runnable {

	private double maxDistance;
	private double memberLon;
	private double memberLat;
	private long index;
	private long entries;

	/** reference to result aggregator */
	private ConcurrentHashMap<Integer, Map<String, Double>> resultsAggregator;

	/** reference to active workers */
	private ConcurrentHashMap<Integer, Integer> activeWorkers;

	private Map<String, Double> eventIdsDistance = new ConcurrentHashMap<>();

	/**
	 * Constructor
	 * 
	 * @param jobId
	 * @param maxDistance
	 * @param memberLon
	 * @param memberLat
	 * @param index
	 * @param entries
	 * @param resultsAggregator
	 * @param activeWorkers
	 */
	public EventsRecommendationByDistanceWorkerRunnable(int jobId,
			double maxDistance, double memberLon, double memberLat, long index,
			long entries,
			ConcurrentHashMap<Integer, Map<String, Double>> resultsAggregator,
			ConcurrentHashMap<Integer, Integer> activeWorkers) {
		super();
		this.jobId = jobId;
		this.maxDistance = maxDistance;
		this.memberLon = memberLon;
		this.memberLat = memberLat;
		this.index = index;
		this.entries = entries;
		this.resultsAggregator = resultsAggregator;
		this.activeWorkers = activeWorkers;
	}

	/**
	 * Procedure that each worker executes
	 */
	@Override
	public void run() {

		/** Get subset of rows from database */
		Set<Event> events = Dao.getInstance().getPaginatedEvents(getIndex(),
				getEntries());

		Iterator<Event> iter = events.iterator();
		/** parse all rows of events */
		while (iter.hasNext()) {
			Event event = iter.next();
			/** calculate the distance */
			double eventDistance = Utils.distance(memberLat, memberLon,
					event.getLat(), event.getLon(), 'K');

			/** check if event is closer than defined distance from member */
			if (eventDistance < maxDistance) {
				/** add event Id into Result Set */
				eventIdsDistance.put(event.getId(), eventDistance);

			}
		}
		System.out
				.println("[DEBUG] [Map Reduce Worker - "
						+ jobId
						+ "] Filtering Events based on proximity to user within data size of: "
						+ events.size()
						+ " found num of events with critiria: "
						+ eventIdsDistance.size());
		/** save completed job to results aggregator */
		resultsAggregator.put(getJobId(), eventIdsDistance);
		activeWorkers.remove(getJobId());
	}

	public double getMemberLon() {
		return memberLon;
	}

	public void setMemberLon(double memberLon) {
		this.memberLon = memberLon;
	}

	public double getMemberLat() {
		return memberLat;
	}

	public void setMemberLat(double memberLat) {
		this.memberLat = memberLat;
	}

	public long getIndex() {
		return index;
	}

	public void setIndex(long index) {
		this.index = index;
	}

	public long getEntries() {
		return entries;
	}

	public void setEntries(long entries) {
		this.entries = entries;
	}

	public ConcurrentHashMap<Integer, Map<String, Double>> getResultsAggregator() {
		return resultsAggregator;
	}

	public void setResultsAggregator(
			ConcurrentHashMap<Integer, Map<String, Double>> resultsAggregator) {
		this.resultsAggregator = resultsAggregator;
	}

	private int jobId;

	public int getJobId() {
		return jobId;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	public double getMaxDistance() {
		return maxDistance;
	}

	public void setMaxDistance(double maxDistance) {
		this.maxDistance = maxDistance;
	}

}
