package com.aueb.distributed.mapreduce.dto;

/**
 * 
 * Events DTO comes from this query from
 * > SHOW CREATE TABLE meetup.events;
 * 
 * 'CREATE TABLE `events` (
 *   `id` varchar(45) NOT NULL,
 *   `urlname` varchar(150) DEFAULT NULL,
 *   `time` bigint(20) DEFAULT NULL,
 *   `utc_offset` int(10) DEFAULT NULL,
 *   `lat` double DEFAULT NULL,
 *   `lon` double DEFAULT NULL,
 *   PRIMARY KEY (`id`),
 *   KEY `lat_index` (`lat`),
 *   KEY `lon_index` (`lon`),
 *   KEY `time_index` (`time`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8'
 * 
 */
public class Event {
	
	private String id;
	
	private double lat;
	
	private double lon;




	/**
	 * 
	 * @param id
	 * @param lat
	 * @param lon
	 */
	public Event(String id, double lat, double lon) {
		super();
		this.id = id;
		this.lat = lat;
		this.lon = lon;
	}




	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		long temp;
		temp = Double.doubleToLongBits(lat);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(lon);
		result = prime * result + (int) (temp ^ (temp >>> 32));
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
		Event other = (Event) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (Double.doubleToLongBits(lat) != Double.doubleToLongBits(other.lat))
			return false;
		if (Double.doubleToLongBits(lon) != Double.doubleToLongBits(other.lon))
			return false;
		return true;
	}




	public double getLat() {
		return lat;
	}


	public void setLat(double lat) {
		this.lat = lat;
	}


	public double getLon() {
		return lon;
	}


	public void setLon(double lon) {
		this.lon = lon;
	}


	public String getId() {
		return id;
	}



	

}
