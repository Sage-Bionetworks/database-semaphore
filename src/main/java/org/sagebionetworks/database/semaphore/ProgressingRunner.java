package org.sagebionetworks.database.semaphore;

/**
 * An abstraction for a runner that can make progress.
 */
public interface ProgressingRunner {
	
	/**
	 * 
	 * @param progressCallback
	 * @throws Exception
	 */
	public void run(ProgressCallback progressCallback) throws Exception;

}
