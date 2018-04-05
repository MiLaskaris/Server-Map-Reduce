package com.aueb.distributed.mapreduce.service;

import java.util.Map;
import java.util.Set;

/**
 * Adding the use cases of the problem.
 */
public interface MapReduceService {
	
	public Set<String> getEventIdAroundMember(long memberId, double distanceInKm, long eventCount);
	
	public Set<Long> getRecommendedFriendsByEventAttendance(long memberId, long memberCount);
	
}
