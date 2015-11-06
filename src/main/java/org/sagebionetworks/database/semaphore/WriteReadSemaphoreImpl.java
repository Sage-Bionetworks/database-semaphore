package org.sagebionetworks.database.semaphore;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

public class WriteReadSemaphoreImpl implements WriteReadSemaphore {
	
	private static final String CALL_REFRESH_READ_LOCK = "CALL refreshReadLock(?,?,?)";
	private static final String CALL_REFRESH_WRITE_LOCK = "CALL refreshWriteLock(?,?,?)";
	private static final String EXPIRED = "EXPIRED";
	private static final String CALL_ATTEMPT_TO_ACQUIRE_WRITE_LOCK_PRECURSOR = "CALL attemptToAcquireWriteLockPrecursor(?,?)";
	private static final String DELETE_FROM_WRITE_READ_MASTER = "DELETE FROM WRITE_READ_MASTER WHERE LOCK_KEY IS NOT NULL";
	private static final String CALL_RELEASE_WRITE_LOCK = "CALL releaseWriteLock(?,?)";
	private static final String CALL_ATTEMPT_TO_ACQUIRE_WRITE_LOCK = "CALL attemptToAcquireWriteLock(?,?,?)";
	private static final String CALL_RELEASE_READ_LOCK = "CALL releaseReadLock(?,?)";
	private static final String CALL_ATTEMPT_TO_ACQUIRE_READ_LOCK = "CALL attemptToAcquireReadLock(?,?)";
	private static final String PROCEDURE_DDL_SQL_TEMPLATE = "schema/writeread/%s.ddl.sql";
	private static final String PROCEDURE_EXITS_TEMPLATE = "PROCEDURE %s already exists";

	private static final String SEMAPHORE_WRITE_READ_MASTER_DDL_SQL	="schema/writeread/WriteReadMaster.ddl.sql";
	private static final String SEMAPHORE_WRITE_READ_LOCK_DDL_SQL	="schema/writeread/WriteReadLock.ddl.sql";
	private static final String[] PROCEDURE_SCRIPTS = new String[]{
		"lockOnWriteReadMaster",
		"attemptToAcquireReadLock",
		"attemptToAcquireWriteLockPrecursor",
		"attemptToAcquireWriteLock",
		"releaseReadLock",
		"releaseWriteLock",
		"refreshReadLock",
		"refreshWriteLock"
	};
	
	private static final Logger log = LogManager
			.getLogger(WriteReadSemaphoreImpl.class);
	
	private JdbcTemplate jdbcTemplate;
	
	public WriteReadSemaphoreImpl(DataSource dataSourcePool) {
		if (dataSourcePool == null) {
			throw new IllegalArgumentException("DataSource cannot be null");
		}
		jdbcTemplate = new JdbcTemplate(dataSourcePool);
		// Create the tables
		this.jdbcTemplate
				.update(Utils.loadStringFromClassPath(SEMAPHORE_WRITE_READ_MASTER_DDL_SQL));
		this.jdbcTemplate
				.update(Utils.loadStringFromClassPath(SEMAPHORE_WRITE_READ_LOCK_DDL_SQL));
		for(String scriptName: PROCEDURE_SCRIPTS){
			createProcedureIfDoesNotExist(scriptName);
		}
	}
	
	/**
	 * Load the procedure ddl file and create it if it does not exist.
	 * 
	 * @param name
	 */
	private void createProcedureIfDoesNotExist(String name) {
		try {
			this.jdbcTemplate.update(Utils.loadStringFromClassPath(String.format(
					PROCEDURE_DDL_SQL_TEMPLATE, name)));
		} catch (DataAccessException e) {
			String message = String.format(PROCEDURE_EXITS_TEMPLATE, name);
			if (e.getMessage().contains(message)) {
				log.info(message);
			} else {
				throw e;
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.database.semaphore.WriteReadSemaphore#acquireReadLock(java.lang.String, long)
	 */
	@Override
	public String acquireReadLock(String lockKey, long timeoutSec) {
		if (lockKey == null) {
			throw new IllegalArgumentException("Key cannot be null");
		}
		if (timeoutSec < 1) {
			throw new IllegalArgumentException(
					"TimeoutSec cannot be less then one.");
		}
		return jdbcTemplate.queryForObject(CALL_ATTEMPT_TO_ACQUIRE_READ_LOCK,
				String.class, lockKey, timeoutSec);
	}

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.database.semaphore.WriteReadSemaphore#releaseReadLock(java.lang.String, java.lang.String)
	 */
	@Override
	public void releaseReadLock(String lockKey, String token)
			throws LockReleaseFailedException {
		if (lockKey == null) {
			throw new IllegalArgumentException("Key cannot be null");
		}
		if (token == null) {
			throw new IllegalArgumentException(
					"Token cannot be null.");
		}
		int results = jdbcTemplate.queryForObject(CALL_RELEASE_READ_LOCK,
				Integer.class, lockKey, token);
		Utils.validateResults(lockKey, token, results);
	}

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.database.semaphore.WriteReadSemaphore#acquireWriteLock(java.lang.String, java.lang.String, long)
	 */
	@Override
	public String acquireWriteLock(String lockKey,
			String precursorToken, long timeoutSec) {
		if (lockKey == null) {
			throw new IllegalArgumentException("Key cannot be null");
		}
		if (precursorToken == null) {
			throw new IllegalArgumentException("Precursor token cannot be null");
		}
		if (timeoutSec < 1) {
			throw new IllegalArgumentException(
					"TimeoutSec cannot be less then one.");
		}
		String results = jdbcTemplate.queryForObject(CALL_ATTEMPT_TO_ACQUIRE_WRITE_LOCK,
				String.class, lockKey, precursorToken, timeoutSec);
		if(EXPIRED.equals(results)){
			throw new LockExpiredException("Precursor lock has expired for key: " + lockKey);
		}
		return results;
	}

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.database.semaphore.WriteReadSemaphore#releaseWriteLock(java.lang.String, java.lang.String)
	 */
	@Override
	public void releaseWriteLock(String lockKey, String token)
			throws LockReleaseFailedException {
		if (lockKey == null) {
			throw new IllegalArgumentException("Key cannot be null");
		}
		if (token == null) {
			throw new IllegalArgumentException(
					"Token cannot be null.");
		}
		int results = jdbcTemplate.queryForObject(CALL_RELEASE_WRITE_LOCK,
				Integer.class, lockKey, token);
		Utils.validateResults(lockKey, token, results);
	}

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.database.semaphore.WriteReadSemaphore#releaseAllLocks()
	 */
	@Override
	public void releaseAllLocks() {
		jdbcTemplate.update(DELETE_FROM_WRITE_READ_MASTER);
	}

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.database.semaphore.WriteReadSemaphore#acquireWriteLockPrecursor(java.lang.String, long)
	 */
	@Override
	public String acquireWriteLockPrecursor(String lockKey, long timeoutSec) {
		if (lockKey == null) {
			throw new IllegalArgumentException("Key cannot be null");
		}
		if (timeoutSec < 1) {
			throw new IllegalArgumentException(
					"TimeoutSec cannot be less then one.");
		}
		return jdbcTemplate.queryForObject(CALL_ATTEMPT_TO_ACQUIRE_WRITE_LOCK_PRECURSOR,
				String.class, lockKey, timeoutSec);
	}

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.database.semaphore.WriteReadSemaphore#refreshReadLock(java.lang.String, java.lang.String, long)
	 */
	@Override
	public void refreshReadLock(String lockKey, String token, long timeoutSec)
			throws LockExpiredException {
		if (lockKey == null) {
			throw new IllegalArgumentException("Key cannot be null");
		}
		if (token == null) {
			throw new IllegalArgumentException(
					"Token cannot be null.");
		}
		if (timeoutSec < 1) {
			throw new IllegalArgumentException(
					"TimeoutSec cannot be less then one.");
		}
		int results = jdbcTemplate.queryForObject(CALL_REFRESH_READ_LOCK,
				Integer.class, lockKey, token, timeoutSec);
		Utils.validateNotExpired(lockKey, token, results);
	}

	/*
	 * (non-Javadoc)
	 * @see org.sagebionetworks.database.semaphore.WriteReadSemaphore#refreshWriteLock(java.lang.String, java.lang.String, long)
	 */
	@Override
	public void refreshWriteLock(String lockKey, String token, long timeoutSec)
			throws LockExpiredException {
		if (lockKey == null) {
			throw new IllegalArgumentException("Key cannot be null");
		}
		if (token == null) {
			throw new IllegalArgumentException(
					"Token cannot be null.");
		}
		if (timeoutSec < 1) {
			throw new IllegalArgumentException(
					"TimeoutSec cannot be less then one.");
		}
		int results = jdbcTemplate.queryForObject(CALL_REFRESH_WRITE_LOCK,
				Integer.class, lockKey, token, timeoutSec);
		Utils.validateNotExpired(lockKey, token, results);
	}

}
