package com.aueb.distributed.mapreduce.conf;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Configuration {

	/** Singleton of Cofiguration */
	private static Configuration conf;
	
	private static final String CONFIG_DIR = "config";
	private static final String PATH_TO_CONFIG = CONFIG_DIR + java.io.File.separator + "config.properties";

	private String dbDriver = null;
	private String dbUri = null;
	private String dbUsername = null;
	private String dbPassword = null;
	private String numWorkers = null;
	private String numReducers = null;

	
	/**
	 * Gets the singleton of configuration.
	 * 
	 * @return conf
	 */
	public static synchronized Configuration getInstance() {
		return (conf != null) ? conf : new Configuration();
	}


	private Configuration() {		
		synchronized (Configuration.class) {
			if (conf != null) {
				throw new IllegalStateException();
			}
			conf = this;
		}				
		Properties properties = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream(PATH_TO_CONFIG);
			properties.load(input);
			dbDriver = properties.getProperty("dbDriver");
			dbUri = properties.getProperty("dbUrl");
			dbUsername = properties.getProperty("dbUsername");
			dbPassword = properties.getProperty("dbPassword");
			numWorkers = properties.getProperty("numWorkers");
			numReducers = properties.getProperty("numReducers");
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public String getDbDriver() {
		return dbDriver;
	}

	public String getDbUri() {
		return dbUri;
	}

	public String getDbUsername() {
		return dbUsername;
	}

	public String getDbPassword() {
		return dbPassword;
	}

	public String getNumWorkers() {
		return numWorkers;
	}

	public String getNumReducers() {
		return numReducers;
	}
}
