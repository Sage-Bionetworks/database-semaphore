package org.sagebionetworks.database.semaphore;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

public class Utils {

	
	/**
	 * Simple utility to load a class path file as a string.
	 * 
	 * @param fileName
	 * @return
	 */
	public static String loadStringFromClassPath(String fileName) {
		InputStream in = Utils.class.getClassLoader()
				.getResourceAsStream(fileName);
		if (in == null) {
			throw new IllegalArgumentException("Cannot find: " + fileName
					+ " on the classpath");
		}
		try {
			return IOUtils.toString(in, "UTF-8");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Validate the result == 1, indicating a single row was updated.
	 * 
	 * @param key
	 * @param token
	 * @param result
	 * @throws LockKeyNotFoundException for a result < 1
	 * @throws LockReleaseFailedException for a result == 0
	 */
	public static void validateResults(final String key, final String token, int result) {
		if (result < 0) {
			throw new LockKeyNotFoundException("Key not found: " + key);
		} else if (result == 0) {
			throw new LockReleaseFailedException("Key: " + key
					+ " token: " + token + " has expired.");
		}
	}
}
