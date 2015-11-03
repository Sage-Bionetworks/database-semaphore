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
		"setupWriteReadLock",
		"attemptToAcquireReadLock"
	};
	
	private static final String CALL_SETUP_LOCK = "CALL setupWriteReadLock(?)";
	
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
				.update(ClasspathUtils.loadStringFromClassPath(SEMAPHORE_WRITE_READ_MASTER_DDL_SQL));
		this.jdbcTemplate
				.update(ClasspathUtils.loadStringFromClassPath(SEMAPHORE_WRITE_READ_LOCK_DDL_SQL));
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
			this.jdbcTemplate.update(ClasspathUtils.loadStringFromClassPath(String.format(
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
	public String acquireReadLock(String lockKey, long timeoutMS) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void releaseReadLock(String lockKey, String token)
			throws LockReleaseFailedException {
		// TODO Auto-generated method stub

	}

	@Override
	public String acquireWriteLockPrecursor(String lockKey) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String acquireWriteLock(String lockKey,
			String exclusiveLockPrecursorToken, long timeoutMS) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void releaseWriteLock(String lockKey, String token)
			throws LockReleaseFailedException {
		// TODO Auto-generated method stub

	}

	@Override
	public void releaseAllLocks() {
		// TODO Auto-generated method stub

	}

	@Override
	public void setupLock(String lockKey) {
		if(lockKey == null){
			throw new IllegalArgumentException("Lock key cannot be null");
		}
		// Ensure we have a master row for this key.
		jdbcTemplate.update(CALL_SETUP_LOCK, lockKey);
	}

}
