package com.aueb.distributed.mapreduce.dto;


/**
 * 
 * Members DTO comes from this query from
 * > SHOW CREATE TABLE meetup.members;
 * 
 * 'CREATE TABLE `members` (
 * `id` int(11) NOT NULL,
 * `name` varchar(45) DEFAULT NULL,
 * `joined` bigint(20) DEFAULT NULL,
 * `bio` varchar(300) DEFAULT NULL,
 * `country` varchar(45) DEFAULT NULL,
 * `city` varchar(45) DEFAULT NULL,
 * `state` varchar(45) DEFAULT NULL,
 * `email` varchar(45) DEFAULT NULL,
 * `gender` varchar(15) DEFAULT NULL,
 * `hometown` varchar(200) DEFAULT NULL,
 * `lang` varchar(45) DEFAULT NULL,
 * `link` varchar(100) DEFAULT NULL,
 * `facebook` varchar(45) DEFAULT NULL,
 * `flickr` varchar(100) DEFAULT NULL,
 * `tumblr` varchar(45) DEFAULT NULL,
 * `twitter` varchar(45) DEFAULT NULL,
 * `linkedin` varchar(100) DEFAULT NULL,
 * `birth_day` int(11) DEFAULT NULL,
 * `birth_month` int(11) DEFAULT NULL,
 * `birth_year` int(11) DEFAULT NULL,
 * `lat` double(12,8) DEFAULT NULL,
 * `lon` double(12,8) DEFAULT NULL,
 * `fb_name` varchar(45) DEFAULT NULL,
 * `fb_gender` varchar(15) DEFAULT NULL,
 * PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=latin1'
 * 
 */
public class Member {
	
	private long id;
	private String name;
//	private long joined;
//	private String bio;
//	private String city;
//	private String state;
//	private String email;
//	private String gender;
//	private String hometown;
//	private String lang;
//	private String link;
//	private String facebook;
//	private String flickr;
//	private String tumblr;
//	private String twitter;
//	private String linkedin;
//	private int birthDay;
//	private int birthMonth;
//	private int birthYear;
	private double lat;
	private double lon;
//	private String fbName;
//	private String fbGender;
	
	/**
	 * 
	 * @param id
	 * @param name
	 * @param lat
	 * @param lon
	 */
	public Member(long id, String name, double lat, double lon) {
		super();
		this.id = id;
		this.name = name;
		this.lat = lat;
		this.lon = lon;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		long temp;
		temp = Double.doubleToLongBits(lat);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(lon);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((name == null) ? 0 : name.hashCode());
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
		Member other = (Member) obj;
		if (id != other.id)
			return false;
		if (Double.doubleToLongBits(lat) != Double.doubleToLongBits(other.lat))
			return false;
		if (Double.doubleToLongBits(lon) != Double.doubleToLongBits(other.lon))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
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



	



}
