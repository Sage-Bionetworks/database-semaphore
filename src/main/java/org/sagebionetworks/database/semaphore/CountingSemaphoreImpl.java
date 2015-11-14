package org.sagebionetworks.database.semaphore;

import static org.sagebionetworks.database.semaphore.Sql.TABLE_SEMAPHORE_LOCK;

import java.sql.Connection;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * <p>
 * Basic database backed multiple lock semaphore. The semaphore involves two
 * tables, a master table, with a single row per unique lock key, and a lock
 * table that can contain multiple tokens for each lock. All operations on the
 * lock table are gated with a SELECT FOR UPDATE on the master table's key row.
 * This ensure all checks and changes occur serially.
 * </p>
 * 
 * Each operation (acquire, release, refresh) is executed using a MySQL
 * procedure. This ensures the amount of time between acquiring the exclusive
 * lock for a key row and the release of the lock is not gated by the
 * health/speed of calling thread. @See <a
 * href="https://sagebionetworks.jira.com/browse/PLFM-3439">PLFM-3439</a>
 * <p>
 * This class is thread-safe and can be used as a singleton.
 * </p>
 * 
 * @author John
 * 
 */

public class CountingSemaphoreImpl implements CountingSemaphore {

	private static final String CALL_REFRESH_SEMAPHORE_LOCK = "CALL refreshSemaphoreLock(?, ?, ?)";

	private static final String CALL_RELEASE_SEMAPHORE_LOCK = "CALL releaseSemaphoreLock(?, ?)";

	private static final String CALL_ATTEMPT_TO_ACQUIRE_SEMAPHORE_LOCK = "CALL attemptToAcquireSemaphoreLock(?, ?, ?)";

	private static final String REFRESH_SEMAPHORE_LOCK = "refreshSemaphoreLock";

	private static final String RELEASE_SEMAPHORE_LOCK = "releaseSemaphoreLock";

	private static final String ATTEMPT_TO_ACQUIRE_SEMAPHORE_LOCK = "attemptToAcquireSemaphoreLock";

	private static final Logger log = LogManager
			.getLogger(CountingSemaphoreImpl.class);

	private static final String SQL_TRUNCATE_LOCKS = "TRUNCATE TABLE "
			+ TABLE_SEMAPHORE_LOCK;

	private static final String SEMAPHORE_LOCK_DDL_SQL = "schema/SemaphoreLock.ddl.sql";
	private static final String SEMAPHORE_MASTER_DDL_SQL = "schema/SemaphoreMaster.ddl.sql";
	private static final String PROCEDURE_DDL_SQL_TEMPLATE = "schema/%s.ddl.sql";
	private static final String PROCEDURE_EXITS_TEMPLATE = "PROCEDURE %s already exists";
	
	/*
	 * All operations for this class require a READ_COMMITED transaction
	 * isolation level.
	 */
	TransactionTemplate readCommitedTransactionTemplate;
	private JdbcTemplate jdbcTemplate;

	/**
	 * Create a new CountingkSemaphore. This implementation depends on two
	 * database tables (SEMAPHORE_MASTER & SEMAPHORE_LOCK) that will be created
	 * if they do not exist.
	 * 
	 * @param dataSourcePool
	 *            Must be a connection to a MySql Database, ideally a database
	 *            connection pool.
	 * @param transactionManager
	 *            The singleton transaction manager.
	 * 
	 */
	public CountingSemaphoreImpl(DataSource dataSourcePool,
			PlatformTransactionManager transactionManager) {
		if (dataSourcePool == null) {
			throw new IllegalArgumentException("DataSource cannot be null");
		}
		jdbcTemplate = new JdbcTemplate(dataSourcePool);
		/*
		 * All operations for this class require a READ_COMMITED transaction
		 * isolation level.
		 */
		readCommitedTransactionTemplate = Utils
				.createReadCommitedTransactionTempalte(transactionManager,
						"CountingSemaphoreImpl");

		// Create the tables
		this.jdbcTemplate.update(Utils
				.loadStringFromClassPath(SEMAPHORE_MASTER_DDL_SQL));
		this.jdbcTemplate.update(Utils
				.loadStringFromClassPath(SEMAPHORE_LOCK_DDL_SQL));
		createProcedureIfDoesNotExist(ATTEMPT_TO_ACQUIRE_SEMAPHORE_LOCK);
		createProcedureIfDoesNotExist(RELEASE_SEMAPHORE_LOCK);
		createProcedureIfDoesNotExist(REFRESH_SEMAPHORE_LOCK);
	}

	/**
	 * Load the procedure ddl file and create it if it does not exist.
	 * 
	 * @param name
	 */
	private void createProcedureIfDoesNotExist(String name) {
		try {
			this.jdbcTemplate.update(Utils.loadStringFromClassPath(String
					.format(PROCEDURE_DDL_SQL_TEMPLATE, name)));
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
	 * 
	 * @see
	 * org.sagebionetworks.warehouse.workers.semaphore.MultipleLockSemaphore
	 * #attemptToAquireLock(java.lang.String, long, int)
	 */
	@Override
	public String attemptToAcquireLock(final String key, final long timeoutSec,
			final int maxLockCount) {
		if (key == null) {
			throw new IllegalArgumentException("Key cannot be null");
		}
		if (timeoutSec < 1) {
			throw new IllegalArgumentException(
					"TimeoutSec cannot be less then one.");
		}
		if (maxLockCount < 1) {
			throw new IllegalArgumentException(
					"MaxLockCount cannot be less then one.");
		}
		return readCommitedTransactionTemplate
				.execute(new TransactionCallback<String>() {
					@Override
					public String doInTransaction(TransactionStatus status) {
						return jdbcTemplate.queryForObject(
								CALL_ATTEMPT_TO_ACQUIRE_SEMAPHORE_LOCK,
								String.class, key, timeoutSec, maxLockCount);
					}
				});

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.sagebionetworks.warehouse.workers.semaphore.MultipleLockSemaphore
	 * #releaseLock(java.lang.String, java.lang.String)
	 */
	@Override
	public void releaseLock(final String key, final String token) {
		if (key == null) {
			throw new IllegalArgumentException("Key cannot be null");
		}
		if (token == null) {
			throw new IllegalArgumentException("Token cannot be null.");
		}
		readCommitedTransactionTemplate.execute(new TransactionCallback<Void>() {
			@Override
			public Void doInTransaction(TransactionStatus status) {
				int result = jdbcTemplate.queryForObject(CALL_RELEASE_SEMAPHORE_LOCK,
						Integer.class, key, token);
				Utils.validateResults(key, token, result);
				return null;
			}
		});

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.sagebionetworks.warehouse.workers.semaphore.MultipleLockSemaphore
	 * #releaseAllLocks()
	 */
	public void releaseAllLocks() {
		jdbcTemplate.update(SQL_TRUNCATE_LOCKS);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.sagebionetworks.warehouse.workers.semaphore.MultipleLockSemaphore
	 * #refreshLockTimeout(java.lang.String, java.lang.String, long)
	 */
	public void refreshLockTimeout(final String key, final String token,
			final long timeoutSec) {
		if (key == null) {
			throw new IllegalArgumentException("Key cannot be null");
		}
		if (token == null) {
			throw new IllegalArgumentException("Token cannot be null.");
		}
		if (timeoutSec < 1) {
			throw new IllegalArgumentException(
					"TimeoutSec cannot be less then one.");
		}
		readCommitedTransactionTemplate.execute(new TransactionCallback<Void>() {
			@Override
			public Void doInTransaction(TransactionStatus status) {
				int result = jdbcTemplate.queryForObject(CALL_REFRESH_SEMAPHORE_LOCK,
						Integer.class, key, token, timeoutSec);
				Utils.validateResults(key, token, result);
				return null;
			}
		});
	}

}
