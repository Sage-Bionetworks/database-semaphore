package org.sagebionetworks.database.semaphore;

/**
 * Callback for worker to notify its container that it has made progress. This
 * allows the container to refresh the timeout on any resource being held on
 * behalf of the worker.
 *
 */
public interface ProgressCallback {

	/**
	 * Notify the container that progress had been made.
	 */
	public void progressMade();

}
