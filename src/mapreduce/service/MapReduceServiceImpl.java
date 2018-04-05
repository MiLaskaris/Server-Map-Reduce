package com.aueb.distributed.mapreduce.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.aueb.distributed.mapreduce.conf.Configuration;
import com.aueb.distributed.mapreduce.db.Dao;
import com.aueb.distributed.mapreduce.dto.Member;
import com.aueb.distributed.mapreduce.workers.EventsRecommendationByDistanceWorkerRunnable;
import com.aueb.distributed.mapreduce.workers.FriendsRecommendationsByAttendanceWorkerRunnable;

/**
 * 
 * Implementation of map reduce algorithm.
 *
 */
public class MapReduceServiceImpl implements MapReduceService {
	
	
	/**
	 * Simple blocking reducer
	 * 
	 * @param activeWorkers
	 * @param resultsAggregator
	 * @return set of all event ids.
	 */
	private Map<String, Double> reducedResultsDouble(
			ConcurrentHashMap<Integer, Integer> activeWorkers,
			ConcurrentHashMap<Integer, Map<String, Double>> resultsAggregator) {

		/** HashMap of results to be returned */
		Map<String, Double> result = new HashMap<String, Double>();

		monitorProgress(activeWorkers);

		result = combineMapsD(resultsAggregator);

		return result;
	}
	
	
	/**
	 * Simple blocking reducer
	 * 
	 * @param activeWorkers
	 * @param resultsAggregator
	 * @return set of all event ids.
	 */
	private Map<Long, Long> reducedResults(
			ConcurrentHashMap<Integer, Integer> activeWorkers,
			ConcurrentHashMap<Integer, Map<Long, Long>> resultsAggregator) {

		/** HashMap of results to be returned */
		Map<Long, Long> result = new HashMap<Long, Long>();

		monitorProgress(activeWorkers);

		result = combineMaps(resultsAggregator);

		return result;
	}

	/**
	 * Wait for all workers threads to finish (max 5 minutes)
	 * 
	 * @param activeWorkers
	 */
	private void monitorProgress(
			ConcurrentHashMap<Integer, Integer> activeWorkers) {
		/** TODO change this to non-blocking.. wait for all threads to finish */
		int i = 0;
		while (activeWorkers.size() > 0) {
			/** wait another 500 ms */
			try {
				/** if waiting is over 10 min abandon waiting */
				if (i > 1200) {
					break;
				}
				Thread.sleep(500);
				i++;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}


	private Map<String, Double> combineMapsD(
			ConcurrentHashMap<Integer, Map<String, Double>> resultsAggregator) {

		/** Hash Set to be returned */
		Map<String, Double> result = new HashMap<String, Double>();

		/**
		 * We reach this point if all threads of workers have finished or max
		 * delay has being reached (1 minute).
		 * 
		 * We are reducing here with one thread.
		 */
		Collection<Map<String, Double>> allResults = resultsAggregator.values();
		Iterator<Map<String, Double>> iter = allResults.iterator();

		/** loop through the maps */
		while (iter.hasNext()) {
			Map<String, Double> intemediateResult = iter.next();
			/** add map result to all */
			Iterator<String> iterMapKeys = intemediateResult.keySet().iterator();
			while (iterMapKeys.hasNext()) {
				String intermediateMapKey = iterMapKeys.next();
				if (result.get(intermediateMapKey) == null) {
					result.put(intermediateMapKey,
							intemediateResult.get(intermediateMapKey));
				} 
			}
		}
		return result;
	}
	
	
	/**
	 * Reducing map results.
	 * 
	 * @param resultsAggregator
	 * @return Map
	 */
	private Map<Long, Long> combineMaps(
			ConcurrentHashMap<Integer, Map<Long, Long>> resultsAggregator) {

		/** Hash Set to be returned */
		Map<Long, Long> result = new HashMap<Long, Long>();

		/**
		 * We reach this point if all threads of workers have finished or max
		 * delay has being reached (1 minute).
		 * 
		 * We are reducing here with one thread.
		 */
		Collection<Map<Long, Long>> allResults = resultsAggregator.values();
		Iterator<Map<Long, Long>> iter = allResults.iterator();

		/** loop through the maps */
		while (iter.hasNext()) {
			Map<Long, Long> intemediateResult = iter.next();
			/** add map result to all */
			Iterator<Long> iterMapKeys = intemediateResult.keySet().iterator();
			while (iterMapKeys.hasNext()) {
				Long intermediateMapKey = iterMapKeys.next();
				if (result.get(intermediateMapKey) == null) {
					result.put(intermediateMapKey,
							intemediateResult.get(intermediateMapKey));
				} else {
					Long resultStored = result.get(intermediateMapKey);
					result.put(intermediateMapKey,
							(resultStored + intemediateResult
									.get(intermediateMapKey)));
				}
			}
		}
		return result;
	}

	/**
	 * Recommendation on events attendance.
	 */
	@Override
	public Set<Long> getRecommendedFriendsByEventAttendance(long memberId,
			long memberCount) {
		/** set to be returned */
		Map<Long, Long> recommendedFriends = new HashMap<Long, Long>();
		Set<Long> friendsResults = new HashSet<Long>();

		/** 1. Get member from database */
		Member member = Dao.getInstance().findMemberById(memberId);

		/** 2. Get events that user has attended */
		Set<String> eventsAttended = Dao.getInstance().getEventsAttended(
				memberId);

		/** 3. Get total count of events */
		long totalEvents = eventsAttended.size();

		/** 4. Calculate number of events to be processed per worker */
		int workersNum = Integer.parseInt(Configuration.getInstance()
				.getNumWorkers());
		long eventsPerWorker = (long) Math.ceil(Double.valueOf(totalEvents)
				/ Double.valueOf(workersNum));

		/** 5. Initialize Executor Service with number of workers */
		ExecutorService workersExecutorService = Executors
				.newFixedThreadPool(workersNum);

		/** 6. Create results structure thread synchronization */
		/** results aggregator **/
		ConcurrentHashMap<Integer, Map<Long, Long>> resultsAggregator = new ConcurrentHashMap<>();
		/** storage of workers for re-initialization */
		ConcurrentHashMap<Integer, FriendsRecommendationsByAttendanceWorkerRunnable> workersStorage = new ConcurrentHashMap<>();
		/** set that keeps workers that have not finished */
		ConcurrentHashMap<Integer, Integer> activeWorkers = new ConcurrentHashMap<>();

		/** 7. Dispatch all worker threads */
		for (int i = 0; i < workersNum; i++) {

			/** Update the active workers registry */
			activeWorkers.put(i, i);

			/** initialization of recommendation worker */
			FriendsRecommendationsByAttendanceWorkerRunnable workerRunnable = new FriendsRecommendationsByAttendanceWorkerRunnable(
					i, memberId, i * eventsPerWorker, eventsPerWorker,
					resultsAggregator, activeWorkers);

			/** we store runnable in case something goes wrong */
			workersStorage.put(i, workerRunnable);

			/** dispatch thread */
			workersExecutorService.execute(workerRunnable);
		}

		/**
		 * 7. Reducer Periodic Thread that runs every 10 secs which checks the
		 * results TODO need to implement the re-initialization of stored
		 * Threads that have failed - failure scenario.
		 * 
		 * We are going to change this to asynchronous callback but for now we
		 * are going to have a synchronized blocking logic with while
		 * */

		recommendedFriends = reducedResults(activeWorkers, resultsAggregator);

		/** transform the result in order to be prepared for showing */
		Map<Long, Set<Long>> recommendedFriendsTransformed = new HashMap<Long, Set<Long>>();

		/** we need to aggregate the results */
		recommendedFriendsTransformed = transformFriends(recommendedFriends);

		friendsResults = getNumOfResults(recommendedFriendsTransformed,
				memberCount);

		/** return a set of user with maximum occurencies on basis of events */
		return friendsResults;
	}

	
	/**
	 * We need to return the number of results of memberCount order by occurencies
	 * 
	 * @param recommendedFriendsTransformed
	 * @param memberCount
	 * @return set of member ids
	 */
	private Set<Long> getNumOfResults(
			Map<Long, Set<Long>> recommendedFriendsTransformed, long memberCount) {
		long membersLeft = memberCount;
		Set<Long> suggestedFriends = new HashSet<>();
		/**
		 * Algorithms first get the max event count.
		 * 
		 * Count down and choose members to include into set.
		 */
		Iterator<Long> iter = recommendedFriendsTransformed.keySet().iterator();
		Long maxValue = Long.valueOf(0);

		while (iter.hasNext()) {
			Long count = iter.next();
			if (count > maxValue) {
				maxValue = count;
			}
		}

		for (long i = maxValue; i > 0; i--) {
			if (recommendedFriendsTransformed.get(i) != null) {
				System.out.println("[DEBUG] Aggregating on event count: " + i);
				Set<Long> intermediate = recommendedFriendsTransformed.get(i);
				if (intermediate.size() < membersLeft) {
					if (membersLeft > 0) {
						suggestedFriends.addAll(intermediate);
						membersLeft = membersLeft - intermediate.size();
					}
				} else {
					if (membersLeft > 0) {
						/** Intermediate size bigger than left size */
						List<Long> list = new ArrayList<>(intermediate);
						Set<Long> subSet = new LinkedHashSet<>(list.subList(0,
								(int) membersLeft));
						membersLeft = membersLeft - intermediate.size();
						suggestedFriends.addAll(subSet);
						break;
					}
				}
			}
		}
		return suggestedFriends;
	}

	/**
	 * Get friends transfored on counts basis.
	 * 
	 * @param recommendedFriends
	 * @return Members per count of occurencies
	 */
	private Map<Long, Set<Long>> transformFriends(
			Map<Long, Long> recommendedFriends) {
		Map<Long, Set<Long>> friendsTransformed = new HashMap<Long, Set<Long>>();

		Iterator<Long> iter = recommendedFriends.keySet().iterator();
		while (iter.hasNext()) {
			Long memberId = iter.next();
			Long eventCount = recommendedFriends.get(memberId);
			if (friendsTransformed.get(eventCount) == null) {
				Set<Long> members = new HashSet<>();
				friendsTransformed.put(eventCount, members);
			} else {
				Set<Long> members = friendsTransformed.get(eventCount);
				members.add(memberId);
				friendsTransformed.put(eventCount, members);
			}
		}
		return friendsTransformed;
	}

	/**
	 * This is the only map reduce method we are going to implement as the
	 * deliverable of this assignment.
	 * 
	 */
	@Override
	public Set<String> getEventIdAroundMember(long memberId,
			double distanceInKm, long eventCount) {
		
		Map<String, Double> eventIdsAroundMember = new HashMap<String, Double>();


		/** 1. Get member from database */
		Member member = Dao.getInstance().findMemberById(memberId);

		/** member latitude */
		double memberLat = member.getLat();
		/** member longitude */
		double memberLon = member.getLon();

		System.out.println("[DEBUG] Calculating events that are maximum "
				+ distanceInKm + " Km from member with id: " + memberId
				+ " and name: " + member.getName());

		/** 2. Get total count of events */
		long totalEvents = Dao.getInstance().countEvents();

		/** 3. Calculate number of events to be processed per worker */
		int workersNum = Integer.parseInt(Configuration.getInstance()
				.getNumWorkers());
		long eventsPerWorker = totalEvents / workersNum;

		/** 4. Initialize Executor Service with number of workers */
		ExecutorService workersExecutorService = Executors
				.newFixedThreadPool(workersNum);

		/** 5. Create results structure thread synchronization */

		/** results aggregator **/
		ConcurrentHashMap<Integer, Map<String, Double>> resultsAggregator = new ConcurrentHashMap<>();
		/** storage of workers for re-initialization */
		ConcurrentHashMap<Integer, EventsRecommendationByDistanceWorkerRunnable> workersStorage = new ConcurrentHashMap<>();
		/** set that keeps workers that have not finished */
		ConcurrentHashMap<Integer, Integer> activeWorkers = new ConcurrentHashMap<>();

		/** 6. Dispatch all worker threads */
		for (int i = 0; i < workersNum; i++) {
			activeWorkers.put(i, i);
			 EventsRecommendationByDistanceWorkerRunnable workerRunnable = new EventsRecommendationByDistanceWorkerRunnable(i,
			 distanceInKm, memberLon, memberLat,  i * eventsPerWorker,
			 eventsPerWorker, resultsAggregator, activeWorkers);

			/** we store runnable in case something goes wrong */
			 workersStorage.put(i, workerRunnable);
			
			 /** dispatch thread */
			 workersExecutorService.execute(workerRunnable);
		}
		

		eventIdsAroundMember = reducedResultsDouble(activeWorkers, resultsAggregator);
		
		Set<String> filteredEvents = filter(eventIdsAroundMember, eventCount );
		
		return filteredEvents;
	}


	/**
	 * Filter results based events counts
	 * @param eventIdsAroundMember
	 * @param eventCount
	 * @return Set String
	 */
	private Set<String> filter(Map<String, Double> eventIdsAroundMember,
			long eventCount) {
		long eventsToBeAdded = eventCount;
		Set<String> events = new HashSet<>();
		Iterator<String> eventsIter = eventIdsAroundMember.keySet().iterator();
		while (eventsIter.hasNext()) {
			String eventId = eventsIter.next();
			/** if distance of event smaller */
			if (eventsToBeAdded > 0) {
				events.add(eventId);
				eventsToBeAdded--;
			}
		}
		return events;
	}

}
