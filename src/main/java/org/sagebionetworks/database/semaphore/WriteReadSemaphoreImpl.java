package org.sagebionetworks.database.semaphore;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

public class WriteReadSemaphoreImpl implements WriteReadSemaphore {
	
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

	@Override
	public String acquireReadLock(String lockKey, long timeoutSec) {
		if (lockKey == null) {
			throw new IllegalArgumentException("Key cannot be null");
		}
		if (timeoutSec < 1) {
			throw new IllegalArgumentException(
					"TimeoutSec cannot be less then one.");
		}
		return jdbcTemplate.queryForObject("CALL attemptToAcquireReadLock(?,?)",
				String.class, lockKey, timeoutSec);
	}

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
		int results = jdbcTemplate.queryForObject("CALL releaseReadLock(?,?)",
				Integer.class, lockKey, token);
		Utils.validateResults(lockKey, token, results);
	}

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
		String results = jdbcTemplate.queryForObject("CALL attemptToAcquireWriteLock(?,?,?)",
				String.class, lockKey, precursorToken, timeoutSec);
		if("EXPIRED".equals(results)){
			throw new LockExpiredException("Precursor lock has expired for key: " + lockKey);
		}
		return results;
	}

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
		int results = jdbcTemplate.queryForObject("CALL releaseWriteLock(?,?)",
				Integer.class, lockKey, token);
		Utils.validateResults(lockKey, token, results);
	}

	@Override
	public void releaseAllLocks() {
		jdbcTemplate.update("DELETE FROM WRITE_READ_MASTER WHERE LOCK_KEY IS NOT NULL");
	}

	@Override
	public String acquireWriteLockPrecursor(String lockKey, long timeoutSec) {
		if (lockKey == null) {
			throw new IllegalArgumentException("Key cannot be null");
		}
		if (timeoutSec < 1) {
			throw new IllegalArgumentException(
					"TimeoutSec cannot be less then one.");
		}
		return jdbcTemplate.queryForObject("CALL attemptToAcquireWriteLockPrecursor(?,?)",
				String.class, lockKey, timeoutSec);
	}

}
