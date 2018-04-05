package com.aueb.distributed.mapreduce.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import com.aueb.distributed.mapreduce.conf.Configuration;



/**
 * Simple connection factory
 * 
 */
public class ConnectionFactory {

	public static final String DRIVER = Configuration.getInstance().getDbDriver();

	public static final String CONNECTION_URL = Configuration.getInstance().getDbUri();

	public static final String CONNECTION_USERNAME = Configuration.getInstance().getDbUsername();

	public static final String CONNECTION_PASSWORD = Configuration.getInstance().getDbPassword();

	String driverClassName = DRIVER;
	String connectionUrl = CONNECTION_URL;
	String dbUser = CONNECTION_USERNAME;
	String dbPwd = CONNECTION_PASSWORD;

	private static ConnectionFactory connectionFactory = null;

	private ConnectionFactory() {
		try {
			Class.forName(driverClassName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public Connection getConnection() throws SQLException {
		Connection conn = null;
		conn = DriverManager.getConnection(connectionUrl, dbUser, dbPwd);
		return conn;
	}

	public static ConnectionFactory getInstance() {
		if (connectionFactory == null) {
			connectionFactory = new ConnectionFactory();
		}
		return connectionFactory;
	}
}