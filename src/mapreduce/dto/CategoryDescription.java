package com.aueb.distributed.mapreduce.dto;


/**
 * Categories description DTO comes from this query from
 * > SHOW CREATE TABLE meetup.category_desc;
 * 
 * 'CREATE TABLE `category_desc` (
 * `id` int(11) NOT NULL,
 * `name` varchar(45) DEFAULT NULL,
 * `shortname` varchar(45) DEFAULT NULL,
 * PRIMARY KEY (`id`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8'
 *  
 */
public class CategoryDescription {
	
	private long id;

	private String name;
	
	private String shortName;

	/**
	 * 
	 * @param id
	 * @param name
	 * @param shortName
	 */
	public CategoryDescription(long id, String name, String shortName) {
		super();
		this.id = id;
		this.name = name;
		this.shortName = shortName;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result
				+ ((shortName == null) ? 0 : shortName.hashCode());
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
		CategoryDescription other = (CategoryDescription) obj;
		if (id != other.id)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (shortName == null) {
			if (other.shortName != null)
				return false;
		} else if (!shortName.equals(other.shortName))
			return false;
		return true;
	}
	

	
	
}
