package com.aueb.distributed.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.TimeZone;

/**
 * 
 * Calculation Utils
 *
 */
public class Utils {
	
	/**
	 * Calculates distance of two points 
	 * 
	 * @param latPoint1 
	 * @param lonPoint1
	 * @param latPoint2
	 * @param lonPoint2
	 * @param unit (M: for miles, K: for kilometers, N: for nautical miles)
	 * @return distance between points
	 */
	public static final double distance(double latPoint1, double lonPoint1, double latPoint2, double lonPoint2, char unit)
	{
	    double theta = lonPoint1 - lonPoint2;
	    double dist = Math.sin(deg2rad(latPoint1)) * Math.sin(deg2rad(latPoint2)) + Math.cos(deg2rad(latPoint1)) * Math.cos(deg2rad(latPoint2)) * Math.cos(deg2rad(theta));
	    dist = Math.acos(dist);
	    dist = rad2deg(dist);
	    dist = dist * 60 * 1.1515;
	     
	    if (unit == 'K') {
	        dist = dist * 1.609344;
	    }
	    else if (unit == 'N') {
	        dist = dist * 0.8684;
	    }
	     
	    return (dist);
	}
	 
	/**
	 * <p>This function converts decimal degrees to radians.</p>
	 * 
	 * @param deg - the decimal to convert to radians
	 * @return the decimal converted to radians
	 */
	private static final double deg2rad(double deg)
	{
	    return (deg * Math.PI / 180.0);
	}
	 
	/**
	 * <p>This function converts radians to decimal degrees.</p>
	 * 
	 * @param rad - the radian to convert
	 * @return the radian converted to decimal degrees
	 */
	private static final double rad2deg(double rad)
	{
	    return (rad * 180 / Math.PI);
	}
	
	/**
	 * 
	 */
	public static <T> String buildCommaSeparatedString(Collection<T> values) {
	    if (values==null || values.isEmpty()) return "";
	    StringBuilder result = new StringBuilder();
	    for (T val : values) {
	        result.append(val);
	        result.append(",");
	    }
	    return result.substring(0, result.length() - 1);
	}

	/**
	 * A common way to calculate the date times in the db.
	 * 
	 * @param date
	 * @return date in UTC format below.
	 */
	public static String getDateForDB(Date date) {
		if (date == null) {
			return null;
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		return sdf.format(date);
	}

	/**
	 * A common way revert the convertion.
	 * 
	 * @param date
	 * @return date in UTC format below.
	 */
	public static Date getDateForString(String date) {
		if (date == null) {
			return null;
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		Date dateConverted = null;
		try {
			dateConverted = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return dateConverted;
	}

	/**
	 * Get time difference between two dates in days, hours, minutes, seconds.
	 * 
	 * @param dateOne
	 * @param dateTwo
	 * @return
	 */
	public static String getTimeDiff(Date dateOne, Date dateTwo) {
		long diff = dateTwo.getTime() - dateOne.getTime();
		long diffSeconds = diff / 1000 % 60;
		long diffMinutes = diff / (60 * 1000) % 60;
		long diffHours = diff / (60 * 60 * 1000) % 24;
		long diffDays = diff / (24 * 60 * 60 * 1000);
		return String.format("%d day(s) %d hour(s) %d min(s) %d sec(s)", diffDays, diffHours, diffMinutes, diffSeconds);
	}
	
}
