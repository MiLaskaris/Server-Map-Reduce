package com.aueb.distributed.mapreduce.dto;


/**
 * 
 * Attends DTO comes from this query from
 * > SHOW CREATE TABLE meetup.attends;
 * 
 * 'CREATE TABLE `attends` (
 * `member_id` bigint(20) NOT NULL,
 * `event_id` varchar(45) NOT NULL,
 * `response` varchar(10) DEFAULT NULL,
 * PRIMARY KEY (`member_id`,`event_id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8'
 * 
 */
public class Attend {
	
	private long memberId;
	
	private long eventId;
	
	private String response;

	/**
	 * 
	 * @param memberId
	 * @param eventId
	 * @param response
	 */
	public Attend(long memberId, long eventId, String response) {
		super();
		this.memberId = memberId;
		this.eventId = eventId;
		this.response = response;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (eventId ^ (eventId >>> 32));
		result = prime * result + (int) (memberId ^ (memberId >>> 32));
		result = prime * result
				+ ((response == null) ? 0 : response.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Attend other = (Attend) obj;
		if (eventId != other.eventId)
			return false;
		if (memberId != other.memberId)
			return false;
		if (response == null) {
			if (other.response != null)
				return false;
		} else if (!response.equals(other.response))
			return false;
		return true;
	}
	
	
	
}
