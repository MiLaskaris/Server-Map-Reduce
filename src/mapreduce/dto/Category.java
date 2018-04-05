package com.aueb.distributed.mapreduce.dto;


/**
 * Categories DTO comes from this query from
 * > SHOW CREATE TABLE meetup.categories;
 * 
 * 'CREATE TABLE `categories` (
 * `cat_id` int(11) NOT NULL,
 * `urlname` varchar(100) NOT NULL,
 * PRIMARY KEY (`cat_id`,`urlname`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8'
 */
public class Category {
	
	private long catId;
	
	private String urlName;

	/**
	 * 
	 * @param catId
	 * @param urlName
	 */
	public Category(long catId, String urlName) {
		super();
		this.catId = catId;
		this.urlName = urlName;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (catId ^ (catId >>> 32));
		result = prime * result + ((urlName == null) ? 0 : urlName.hashCode());
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
		Category other = (Category) obj;
		if (catId != other.catId)
			return false;
		if (urlName == null) {
			if (other.urlName != null)
				return false;
		} else if (!urlName.equals(other.urlName))
			return false;
		return true;
	}
	
	
	
	
	

}
