package com.aueb.distributed.mapreduce.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.aueb.distributed.mapreduce.dto.Event;
import com.aueb.distributed.mapreduce.dto.Member;
import com.aueb.distributed.util.Utils;

/**
 * 
 *  If db connections throws error
 *  
 *	set global max_allowed_packet=33554432;
 *  show variables;
 *
 */
public class Dao {

	private static Dao instance;

	public static synchronized Dao getInstance() {
		if (instance == null) {
			instance = new Dao();
		}
		return instance;
	}

	/**
	 * Gets the connection.
	 * 
	 * @return conn
	 * @throws SQLException
	 */
	private Connection getConnection() throws SQLException {
		Connection conn;
		conn = ConnectionFactory.getInstance().getConnection();
		return conn;
	}

	/**
	 * Find Member by memberId
	 * 
	 * @param memberId
	 */
	public Member findMemberById(long memberId) {
		Connection con = null;
		PreparedStatement ptmt = null;
		ResultSet rs = null;
		Member memberInDB = null;
		try {
			String querystring = "SELECT id, name,lat, lon FROM members m WHERE id=?";			
			con = getConnection();
			ptmt = con.prepareStatement(querystring);
			ptmt.setInt(1, (int) memberId);
			rs = ptmt.executeQuery();
			/** we expect one result */
			if (rs.next()) {				
				long retrievedId = rs.getInt(1);
				String retrievedName = rs.getString(2);
				double retrievedLat = rs.getDouble(3);
				double retrievedLon = rs.getDouble(4);
				memberInDB = new Member(retrievedId, retrievedName,
						retrievedLat, retrievedLon);
				return rs.wasNull() ? null : memberInDB;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ptmt != null)
					ptmt.close();
				if (con != null)
					con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		return memberInDB;
	}
	
	
	/**
	 * Count total events
	 * 
	 * @return total count of events
	 */
	public long countEvents() {
		Connection con = null;
		PreparedStatement ptmt = null;
		ResultSet rs = null;
		long retrievedCount = 0;
		try {
			String querystring = "SELECT COUNT(*) FROM events";
			con = getConnection();
			ptmt = con.prepareStatement(querystring);
			rs = ptmt.executeQuery();
			if (rs.next()) {
				retrievedCount = rs.getInt(1);
				return rs.wasNull() ? null : retrievedCount;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ptmt != null)
					ptmt.close();
				if (con != null)
					con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return retrievedCount;
	}
	
	
	/**
	 * Get events paginated for workers
	 * 
	 * Set of events
	 */
	public Set<Event> getPaginatedEvents(long initIndex, long numEvents) {
		Connection con = null;
		PreparedStatement ptmt = null;
		ResultSet rs = null;
		Set<Event> events = new HashSet<>();
		try {
			String querystring = "SELECT id, lat, lon FROM events  LIMIT " + initIndex + "," + numEvents;
			con = getConnection();
			ptmt = con.prepareStatement(querystring);
			rs = ptmt.executeQuery();
			while (rs.next()) {
				String retrievedId = rs.getString(1);
				double retrievedLat = rs.getDouble(2);
				double retrievedLon = rs.getDouble(3);
				Event event = new Event(retrievedId, retrievedLat, retrievedLon);
				events.add(event);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ptmt != null)
					ptmt.close();
				if (con != null)
					con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return events;
	}
	
	
	/**
	 * Get Paginated Events Id
	 * 
	 * @param initIndex
	 * @param numEvents
	 * @return HashSet
	 */
	public Set<String> getPaginatedEventsId(long initIndex, long numEvents) {
		Connection con = null;
		PreparedStatement ptmt = null;
		ResultSet rs = null;
		HashSet<String> eventIds = new HashSet<>();
		try {
			String querystring = "SELECT id FROM events  LIMIT " + initIndex + "," + numEvents;
			con = getConnection();
			ptmt = con.prepareStatement(querystring);
			rs = ptmt.executeQuery();
			while (rs.next()) {				
				String retrievedId = rs.getString(1);
				eventIds.add('"'+retrievedId+'"');
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ptmt != null)
					ptmt.close();
				if (con != null)
					con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return eventIds;
	}
	
	
	/**
	 * Get Paginated Events Id
	 * 
	 * @param initIndex
	 * @param numEvents
	 * @return HashSet
	 */
	public Map<Long, Long> getMembersAttendanceOfEvent(Set<String> events) {
		Connection con = null;
		PreparedStatement ptmt = null;
		ResultSet rs = null;
		HashMap<Long, Long> membersAttendance = new HashMap<>();
		try {
			String querystring = "SELECT member_id, COUNT(*) FROM attends WHERE event_id IN (" + Utils.buildCommaSeparatedString(events) +") GROUP BY member_id";
			con = getConnection();
			ptmt = con.prepareStatement(querystring);
			rs = ptmt.executeQuery();
			while (rs.next()) {
				long memberId = rs.getInt(1);
				long eventsCounts = rs.getInt(2);
				membersAttendance.put(Long.valueOf(memberId), Long.valueOf(eventsCounts));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ptmt != null)
					ptmt.close();
				if (con != null)
					con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return membersAttendance;
	}
	
	/**
	 * Set relationship between members.
	 * 
	 * @param memberID1
	 * @param memberID2
	 * @param relationship
	 */
	public void setMembersRelationship(long memberID1, long memberID2,
			String relationship) {
		Connection con = null;
		PreparedStatement ptmt = null;
		ResultSet rs = null;
		try {
			String querystring = "INSERT INTO members_relationship(member_id1, member_id2, relationship, time) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE relationship=?";
			con = getConnection();
			ptmt = con.prepareStatement(querystring);
			ptmt.setLong(1, memberID1);
			ptmt.setLong(2, memberID2);
			ptmt.setString(3, relationship);
			Date now = new Date();
			ptmt.setString(4, Utils.getDateForDB(now));
			ptmt.setString(5, relationship);
			ptmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ptmt != null)
					ptmt.close();
				if (con != null)
					con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	
	/**
	 * Set correlation between members based on events attendance.
	 * 
	 * @param memberID1
	 * @param memberID2
	 * @param eventPreferenceMatchingCount
	 */
	public void setMembersCorrelationBasedOnCommonEventsAttendance(long memberID1, long memberID2,
			int eventPreferenceMatchingCount) {
		Connection con = null;
		PreparedStatement ptmt = null;
		ResultSet rs = null;
		try {
			String querystring = "INSERT INTO members_correlation(member_id1, member_id2, events_preference_matching_count) VALUES(?,?,?) ON DUPLICATE KEY UPDATE events_preference_matching_count=?";
			con = getConnection();
			ptmt = con.prepareStatement(querystring);
			ptmt.setLong(1, memberID1);
			ptmt.setLong(2, memberID2);
			ptmt.setInt(3, eventPreferenceMatchingCount);
			ptmt.setInt(4, eventPreferenceMatchingCount);
			ptmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ptmt != null)
					ptmt.close();
				if (con != null)
					con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}


	/**
	 * Get events Attended.
	 * 
	 * @param memberId
	 * @param initIndex
	 * @param numEvents
	 * @return eventID
 	 */
	public Set<String> getEventsAttended(long memberId) {
		Connection con = null;
		PreparedStatement ptmt = null;
		ResultSet rs = null;
		
		HashSet<String> eventIds = new HashSet<>();
		try {
			String querystring = "SELECT event_id FROM attends WHERE member_id = ? AND response = 'yes' ";
			con = getConnection();
			ptmt = con.prepareStatement(querystring);
			ptmt.setLong(1, memberId);
			rs = ptmt.executeQuery();
			while (rs.next()) {				
				String retrievedId = rs.getString(1);
				eventIds.add(retrievedId);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ptmt != null)
					ptmt.close();
				if (con != null)
					con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return eventIds;
	}

	/**
	 * Get Paginated Attended Events.
	 * 
	 * @param memberId
	 * @param index
	 * @param numEvents
	 * @return set of events
	 */
	public Set<String> getPaginatedAttendedEventsId(long memberId, long index,
			long numEvents) {

		Connection con = null;
		PreparedStatement ptmt = null;
		ResultSet rs = null;
		
		HashSet<String> eventIds = new HashSet<>();
		try {
			String querystring = "SELECT event_id FROM attends WHERE member_id = ? AND response = 'yes' LIMIT " + index + "," + numEvents;;
			con = getConnection();
			ptmt = con.prepareStatement(querystring);
			ptmt.setLong(1, memberId);
			rs = ptmt.executeQuery();
			while (rs.next()) {				
				String retrievedId = rs.getString(1);
				eventIds.add(retrievedId);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ptmt != null)
					ptmt.close();
				if (con != null)
					con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return eventIds;
	}
	
	
	/**
	 * Set voting for event
	 * 
	 * @param memberID1
	 * @param memberID2
	 * @param eventPreferenceMatchingCount
	 */
	public void setMembersEventsAttendance(long memberId, long eventId,
			String vote) {
		Connection con = null;
		PreparedStatement ptmt = null;
		ResultSet rs = null;
		try {
			String querystring = "INSERT INTO attends(member_id, event_id, response) VALUES(?,?,?) ON DUPLICATE KEY UPDATE response=?";
			con = getConnection();
			ptmt = con.prepareStatement(querystring);
			ptmt.setLong(1, memberId);
			ptmt.setLong(2, eventId);
			ptmt.setString(3, vote);
			ptmt.setString(4, vote);
			ptmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ptmt != null)
					ptmt.close();
				if (con != null)
					con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	
	/**
	 * 
	 * @param friendsIds
	 * @return
	 */
	public Set<String> getReadableFriends(Set<Long> friendsIds) {
		Connection con = null;
		PreparedStatement ptmt = null;
		ResultSet rs = null;
		Set<String> membersReadableSet = new HashSet<>();
		try {
			String querystring = "SELECT id, name FROM members WHERE id IN (" + Utils.buildCommaSeparatedString(friendsIds) +") ";
			con = getConnection();
			ptmt = con.prepareStatement(querystring);
			rs = ptmt.executeQuery();
			while (rs.next()) {
				long memberId = rs.getInt(1);
				String memberName = rs.getString(2);
				StringBuilder str = new StringBuilder();
				str.append(memberId);
				str.append(",");
				str.append(memberName);
				membersReadableSet.add(str.toString());						
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ptmt != null)
					ptmt.close();
				if (con != null)
					con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return membersReadableSet;
	}

	public Set<Long> getRelationshipsForMember(Long memberId, String relationship) {
		Connection con = null;
		PreparedStatement ptmt = null;
		ResultSet rs = null;
		
		HashSet<Long> relationshipIds = new HashSet<>();
		try {
			String querystring = "SELECT member_id2 FROM members_relationship WHERE relationship = ? AND member_id1= ? union SELECT member_id1 FROM members_relationship WHERE relationship =? AND member_id2= ? ";
			con = getConnection();
			ptmt = con.prepareStatement(querystring);
			ptmt.setString(1, relationship);
			ptmt.setLong(2, memberId);
			ptmt.setString(3, relationship);
			ptmt.setLong(4, memberId);
			rs = ptmt.executeQuery();
			while (rs.next()) {				
				Long retrievedId = rs.getLong(1);
				relationshipIds.add(retrievedId);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ptmt != null)
					ptmt.close();
				if (con != null)
					con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return relationshipIds;
	}
	
	
	public Set<String> getAttendedEventsForMember(Long memberId, String response) {
		Connection con = null;
		PreparedStatement ptmt = null;
		ResultSet rs = null;
		
		HashSet<String> eventsIds = new HashSet<>();
		try {
			String querystring = "SELECT * FROM attends WHERE member_id =? AND response = ?";			
			con = getConnection();
			ptmt = con.prepareStatement(querystring);
			ptmt.setLong(1, memberId);
			ptmt.setString(2, response);
			rs = ptmt.executeQuery();
			while (rs.next()) {				
				String retrievedId = rs.getString(1);
				eventsIds.add(retrievedId);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ptmt != null)
					ptmt.close();
				if (con != null)
					con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return eventsIds;
	}

	public Set<String> getReadableEvents(Set<String> eventIds) {
		Connection con = null;
		PreparedStatement ptmt = null;
		ResultSet rs = null;
		Set<String> eventsReadableSet = new HashSet<>();
		try {
			String querystring = "SELECT id, urlname FROM events WHERE id IN (" + Utils.buildCommaSeparatedString(eventIds) +")";
			con = getConnection();
			ptmt = con.prepareStatement(querystring);
			rs = ptmt.executeQuery();
			while (rs.next()) {
				long eventId = rs.getInt(1);
				String urlName = rs.getString(2);
				StringBuilder str = new StringBuilder();
				str.append(eventId);
				str.append(",");
				str.append(urlName);
				eventsReadableSet.add(str.toString());						
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (ptmt != null)
					ptmt.close();
				if (con != null)
					con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return eventsReadableSet;
	}
	
	
}
