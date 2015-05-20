package org.sagebionetworks.database.semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This is not a singleton. A new instance of this gate must be created each time you need one.
 *
 */
public class SemaphoreGatedRunnerImpl implements SemaphoreGatedRunner {
	
	private static final Logger log = LogManager.getLogger(SemaphoreGatedRunnerImpl.class);

	CountingSemaphore semaphore;
	ProgressingRunner runner;
	String lockKey;
	long lockTimeoutSec = -1;
	int maxLockCount = -1;
	
	/**
	 * 
	 * @param semaphore
	 */
	public SemaphoreGatedRunnerImpl(CountingSemaphore semaphore) {
		super();
		this.semaphore = semaphore;
	}

	/**
	 * This gate must be configured before running.
	 * @param runner
	 * @param lockKey
	 * @param lockTimeoutSec
	 */
	public void configure(ProgressingRunner runner, String lockKey, long lockTimeoutSec, int maxLockCount){
		this.runner = runner;
		this.lockKey = lockKey;
		this.lockTimeoutSec = lockTimeoutSec;
		this.maxLockCount = maxLockCount;
		validateConfig();
	}


	/**
	 * This is the run of the 'runnable'
	 */
	public void run() {
		validateConfig();
		try {
			// attempt to get a lock
			final String lockToken = semaphore.attemptToAcquireLock(this.lockKey, this.lockTimeoutSec, this.maxLockCount);
			// Only proceed if a lock was acquired
			if(lockToken != null){
				try{
					// Let the runner go while holding the lock
					runner.run(new ProgressCallback() {
						public void progressMade() {
							// Give the lock more time
							semaphore.refreshLockTimeout(lockKey, lockToken, lockTimeoutSec);
						}
					});
				}finally{
					semaphore.releaseLock(this.lockKey, lockToken);
				}
			}
		} catch (Throwable e) {
			log.error(e);
		}
	}
	
	private void validateConfig(){
		if(this.runner == null){
			throw new IllegalArgumentException("Runner cannot be be null");
		}
		if(this.lockKey == null){
			throw new IllegalArgumentException("Lock key cannot be null");
		}
		if(lockTimeoutSec < 1){
			throw new IllegalArgumentException("LockTimeoutSec cannot be less than one.");
		}
		if(maxLockCount < 1){
			throw new IllegalArgumentException("MaxLockCount cannot be less than one.");
		}
	}

}
