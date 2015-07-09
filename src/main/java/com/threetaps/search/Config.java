package com.threetaps.search;

import java.util.Iterator;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

public class Config extends CompositeConfiguration {
	private static final Logger log = Logger.getLogger(Config.class);
	private static final Config theInstance = new Config();
	
	protected Config() {
		super();
		/* add local configuration file first, as earlier definitions
		 * override later ones...
		 */
		try {
			addConfiguration(new PropertiesConfiguration("indexd.properties"));
		} catch (ConfigurationException e1) {
			log.info("no indexd.properties; using default configuration");
		}
		try {
			addConfiguration(new PropertiesConfiguration("_indexd.properties"));
		} catch (ConfigurationException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static Config getConfig() {
		return theInstance;
	}
	
	public static String getVersion() {
		return theInstance.getString("version");
	}
	
	public static boolean handleDeletes() {
		return theInstance.getBoolean("handleDeletes");
	}
	
	public static boolean handleReposts() {
		return theInstance.getBoolean("handleReposts");
	}
	
	public static void main(String[] args) {
		for (Iterator<String> keys = theInstance.getKeys(); keys.hasNext(); ) {
			String key = keys.next();
			System.out.println(String.format("%s => %s", key, theInstance.getString(key)));
		}
	}
}
