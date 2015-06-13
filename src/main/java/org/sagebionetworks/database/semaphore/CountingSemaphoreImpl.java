package org.sagebionetworks.database.semaphore;

import static org.sagebionetworks.database.semaphore.Sql.COL_TABLE_SEM_LOCK_EXPIRES_ON;
import static org.sagebionetworks.database.semaphore.Sql.COL_TABLE_SEM_LOCK_LOCK_KEY;
import static org.sagebionetworks.database.semaphore.Sql.COL_TABLE_SEM_LOCK_TOKEN;
import static org.sagebionetworks.database.semaphore.Sql.COL_TABLE_SEM_MAST_KEY;
import static org.sagebionetworks.database.semaphore.Sql.TABLE_SEMAPHORE_LOCK;
import static org.sagebionetworks.database.semaphore.Sql.TABLE_SEMAPHORE_MASTER;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.UUID;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Basic database backed multiple lock semaphore. The semaphore involves two
 * tables, a master table, with a single row per unique lock key, and a lock
 * table that can contain multiple tokens for each lock. All operations on the
 * lock table are gated with a SELECT FOR UPDATE on the master table's key row.
 * This ensure all checks and changes occur serially.
 * 
 * This class is thread-safe and can be used as a singleton.
 * 
 * @author John
 * 
 */

public class CountingSemaphoreImpl implements CountingSemaphore {
	

	private static final Logger log = LogManager.getLogger(CountingSemaphoreImpl.class);
	/*
	 * Errors will be logged if the master lock is held longer than this time in MS.
	 */
	public static final long MAX_LOCK_TIME_MS = 1000;
	private static final String EXCEEDED_THE_MAXIMUM_TIME_TO_ACQUIRE_A_LOCK_IN_MS = "\t %s Exceeded the maximum time the master lock could be held: %d MS";
	
	/*
	 * Debug logging templates.
	 */
	private static final String NO_LOCK_ISSUED_IN_MS	 = "\t %s No Lock issued in %d MS";
	private static final String MASTER_LOCK_HELD_FOR	 = "\t %s Master Lock held for: %d MS";
	/*
	 * Trace logging templates.
	 */
	private static final String CREATED_MASTER			 = "\t %s Created master";
	private static final String REFRESHED_TOKEN			 = "\t %s Refreshed token: %s";
	private static final String FAILED_REFRESH_TOKEN	 = "\t %s Failed refresh token: %s";
	private static final String LOCK_RELEASED_TOKEN		 = "\t %s Lock released token: %s";
	private static final String FAILED_TO_RELEASE_TOKEN	 = "\t %s Failed to release token: %s";
	private static final String LOCKING_ON_MASTER		 = "\t %s Locking on master";
	private static final String NO_LOCKS_AVAILABLE		 = "\t %s No locks available";
	private static final String ISSUED_NEW_LOCK_TOKEN	 = "\t %s Issued new lock token: %s";
	private static final String LOCKS_ISSUED			 = "\t %s Locks issued: %s";
	private static final String DELETED_EXPIRED_LOCKS	 = "\t %s Deleted %s expired locks";

	private static final String SQL_TRUNCATE_LOCKS = "TRUNCATE TABLE "
			+ TABLE_SEMAPHORE_LOCK;

	private static final String SQL_UPDATE_LOCK_EXPIRES = "UPDATE "
			+ TABLE_SEMAPHORE_LOCK + " SET " + COL_TABLE_SEM_LOCK_EXPIRES_ON
			+ " = (CURRENT_TIMESTAMP + INTERVAL ? SECOND) WHERE "
			+ COL_TABLE_SEM_LOCK_LOCK_KEY + " = ? AND "
			+ COL_TABLE_SEM_LOCK_TOKEN + " = ?";

	private static final String SQL_DELETE_LOCK_WITH_TOKEN = "DELETE FROM "
			+ TABLE_SEMAPHORE_LOCK + " WHERE " + COL_TABLE_SEM_LOCK_TOKEN
			+ " = ?";

	private static final String SQL_INSERT_NEW_LOCK = "INSERT INTO "
			+ TABLE_SEMAPHORE_LOCK + "(" + COL_TABLE_SEM_LOCK_LOCK_KEY + ", "
			+ COL_TABLE_SEM_LOCK_TOKEN + ", " + COL_TABLE_SEM_LOCK_EXPIRES_ON
			+ ") VALUES (?, ?, (CURRENT_TIMESTAMP + INTERVAL ? SECOND))";

	private static final String SQL_COUNT_OUTSTANDING_LOCKS = "SELECT COUNT(*) FROM "
			+ TABLE_SEMAPHORE_LOCK
			+ " WHERE "
			+ COL_TABLE_SEM_LOCK_LOCK_KEY
			+ " = ?";

	private static final String SQL_DELETE_EXPIRED_LOCKS = "DELETE FROM "
			+ TABLE_SEMAPHORE_LOCK + " WHERE " + COL_TABLE_SEM_LOCK_LOCK_KEY
			+ " = ? AND " + COL_TABLE_SEM_LOCK_EXPIRES_ON
			+ " < CURRENT_TIMESTAMP";

	private static final String SQL_SELECT_MASTER_KEY_FOR_UPDATE = "SELECT "
			+ COL_TABLE_SEM_MAST_KEY + " FROM " + TABLE_SEMAPHORE_MASTER
			+ " WHERE " + COL_TABLE_SEM_MAST_KEY + " = ? FOR UPDATE";

	private static final String SQL_INSERT_IGNORE_MASTER = "INSERT IGNORE INTO "
			+ TABLE_SEMAPHORE_MASTER
			+ " ("
			+ COL_TABLE_SEM_MAST_KEY
			+ ") VALUES (?)";

	private static final String SEMAPHORE_LOCK_DDL_SQL = "schema/SemaphoreLock.ddl.sql";
	private static final String SEMAPHORE_MASTER_DDL_SQL = "schema/SemaphoreMaster.ddl.sql";
	private static final String ATTEMPT_TO_ACQUIRE_LOCK_PROCEDURE_DDL_SQL = "schema/attemptToAcquireLock.ddl.sql";

	private JdbcTemplate jdbcTemplate;

	DataSource dataSourcePool;

	TransactionTemplate requiresNewTransactionTempalte;

	/**
	 * Create a new CountingkSemaphore. This implementation depends on two
	 * database tables (SEMAPHORE_MASTER & SEMAPHORE_LOCK) that will be created
	 * if they do not exist.
	 * 
	 * @param dataSourcePool
	 *            Must be a connection to a MySql Database, ideally a database
	 *            connection pool.
	 */
	public CountingSemaphoreImpl(DataSource dataSourcePool) {
		if(dataSourcePool == null){
			throw new IllegalArgumentException("DataSource cannot be null");
		}
		// This class should never participate with any other transactions so it
		// gets its own transaction manager.
		DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(
				dataSourcePool);
		DefaultTransactionDefinition transactionDef = new DefaultTransactionDefinition();
		transactionDef.setIsolationLevel(Connection.TRANSACTION_READ_COMMITTED);
		transactionDef.setReadOnly(false);

		transactionDef
				.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
		transactionDef.setName("MultipleLockSemaphoreImpl");
		// This will manage transactions for calls that need it.
		requiresNewTransactionTempalte = new TransactionTemplate(
				transactionManager, transactionDef);

		jdbcTemplate = new JdbcTemplate(dataSourcePool);

		// Create the tables
		this.jdbcTemplate
				.update(loadStringFromClassPath(SEMAPHORE_MASTER_DDL_SQL));
		this.jdbcTemplate
				.update(loadStringFromClassPath(SEMAPHORE_LOCK_DDL_SQL));
		try {
			this.jdbcTemplate
			.update(loadStringFromClassPath(ATTEMPT_TO_ACQUIRE_LOCK_PROCEDURE_DDL_SQL));
		} catch (DataAccessException e) {
			log.info("Procedure 'attemptToAcquireLock' already exists");
		}
	}

	/**
	 * Simple utility to load a class path file as a string.
	 * 
	 * @param fileName
	 * @return
	 */
	public static String loadStringFromClassPath(String fileName) {
		InputStream in = CountingSemaphoreImpl.class.getClassLoader()
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.sagebionetworks.warehouse.workers.semaphore.MultipleLockSemaphore
	 * #attemptToAquireLock(java.lang.String, long, int)
	 */
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
		/*
		 * All of the logic for this called in built into the attemptToAcquireLock procedure (see PLFM-3439)
		 */
		return jdbcTemplate.queryForObject("CALL attemptToAcquireLock(?, ?, ?)", String.class, key, timeoutSec, maxLockCount);
	}

	/**
	 * The transaction where we attempt to acquire the lock.
	 * 
	 * @param key
	 * @param timeoutSec
	 * @param maxLockCount
	 * @return
	 */
	private String attemptToAcquireLockTransaction(final String key,
			final long timeoutSec, final int maxLockCount) {
		// This need to occur in a transaction.
		return requiresNewTransactionTempalte
				.execute(new TransactionCallback<String>() {

					public String doInTransaction(TransactionStatus status) {
						/*
						 * Now lock the master row. This ensure all operations
						 * on this key occur serially.
						 */
						long masterLockAcquiredTimeMS = lockOnMasterKey(key);
						
						// delete expired locks
						long count = jdbcTemplate.update(SQL_DELETE_EXPIRED_LOCKS, key);
						if(log.isTraceEnabled()){
							log.trace(String.format(DELETED_EXPIRED_LOCKS, key, count));
						}
						// Count the remaining locks
						count = jdbcTemplate.queryForObject(
								SQL_COUNT_OUTSTANDING_LOCKS, Long.class, key);
						if(log.isTraceEnabled()){
							log.trace(String.format(LOCKS_ISSUED, key, count));
						}
						if (count < maxLockCount) {
							// issue a lock
							String token = UUID.randomUUID().toString();
							jdbcTemplate.update(SQL_INSERT_NEW_LOCK, key,
									token, timeoutSec);
							
							if(log.isTraceEnabled()){
								log.trace(String.format(ISSUED_NEW_LOCK_TOKEN, key, token));
							}
							validateLockHeldTime(masterLockAcquiredTimeMS, key);
							return token;
						}
						if(log.isTraceEnabled()){
							log.trace(String.format(NO_LOCKS_AVAILABLE, key));
						}
						validateLockHeldTime(masterLockAcquiredTimeMS, key);
						// No token for you!
						return null;
					}
				});
	}
	
	/**
	 * Validate that the master lock was held for only short duration.  If the expected time is exceeded
	 * an error will be logged.
	 * @param masterLockAcquiredTimeMS
	 * @param key
	 */
	private void validateLockHeldTime(long masterLockAcquiredTimeMS, String key){
		// How long was the master lock held?
		long elapseMS = System.currentTimeMillis()- masterLockAcquiredTimeMS;
		if(log.isDebugEnabled()){
			log.debug(String.format(MASTER_LOCK_HELD_FOR, key, elapseMS));
		}
		if(elapseMS > MAX_LOCK_TIME_MS){
			log.error(String.format(EXCEEDED_THE_MAXIMUM_TIME_TO_ACQUIRE_A_LOCK_IN_MS, key, elapseMS));
		}
	}
	
	/**
	 * Lock on the master key row.
	 * @param key
	 * @return The time in MS that the master lock was acquired.
	 * @throws LockKeyNotFoundException if the key does not exist.
	 */
	private long lockOnMasterKey(final String key) {
		try {
			jdbcTemplate.queryForObject(
					SQL_SELECT_MASTER_KEY_FOR_UPDATE, String.class,
					key);
			if(log.isTraceEnabled()){
				log.trace(String.format(LOCKING_ON_MASTER, key));
			}
			return System.currentTimeMillis();
		} catch (EmptyResultDataAccessException e) {
			throw new LockKeyNotFoundException("Key not found: "+key);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.sagebionetworks.warehouse.workers.semaphore.MultipleLockSemaphore
	 * #releaseLock(java.lang.String, java.lang.String)
	 */
	public void releaseLock(final String key, final String token) {
		if (key == null) {
			throw new IllegalArgumentException("Key cannot be null");
		}
		if (token == null) {
			throw new IllegalArgumentException("Token cannot be null.");
		}
		// This need to occur in a transaction.
		requiresNewTransactionTempalte.execute(new TransactionCallback<Void>() {

			public Void doInTransaction(TransactionStatus status) {
				/*
				 * Now lock the master row. This ensure all operations
				 * on this key occur serially.
				 */
				long masterLockAcquiredTimeMS = lockOnMasterKey(key);
				// delete expired locks
				int changes = jdbcTemplate.update(SQL_DELETE_LOCK_WITH_TOKEN,
						token);
				if (changes < 1) {
					if(log.isTraceEnabled()){
						log.trace(String.format(FAILED_TO_RELEASE_TOKEN, key, token));
					}
					validateLockHeldTime(masterLockAcquiredTimeMS, key);
					throw new LockReleaseFailedException("Key: " + key
							+ " token: " + token + " has expired.");
				}
				if(log.isTraceEnabled()){
					log.trace(String.format(LOCK_RELEASED_TOKEN, key, token));
				}
				validateLockHeldTime(masterLockAcquiredTimeMS, key);
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
		// This need to occur in a transaction.
		requiresNewTransactionTempalte.execute(new TransactionCallback<Void>() {

			public Void doInTransaction(TransactionStatus status) {
				/*
				 * Now lock the master row. This ensure all operations
				 * on this key occur serially.
				 */
				long masterLockAcquiredTimeMS = lockOnMasterKey(key);
				// Add more time to the lock.
				int changes = jdbcTemplate.update(SQL_UPDATE_LOCK_EXPIRES,
						timeoutSec, key, token);
				if (changes < 1) {
					if(log.isTraceEnabled()){
						log.trace(String.format(FAILED_REFRESH_TOKEN, key, token));
					}
					validateLockHeldTime(masterLockAcquiredTimeMS, key);
					throw new LockReleaseFailedException("Key: " + key
							+ " token: " + token + " has expired.");
				}
				if(log.isTraceEnabled()){
					log.trace(String.format(REFRESHED_TOKEN, key, token));
				}
				validateLockHeldTime(masterLockAcquiredTimeMS, key);
				return null;
			}
		});
	}

	/**
	 * Create the master lock in its own transaction.
	 * 
	 * @param key
	 */
	private void createLockKeyInTransaction(final String key) {
		if (key == null) {
			throw new IllegalArgumentException("Key cannot be null");
		}
		// This need to occur in a transaction.
		requiresNewTransactionTempalte.execute(new TransactionCallback<Void>() {
			public Void doInTransaction(TransactionStatus status) {
				// step one, ensure we have a master lock
				jdbcTemplate.update(SQL_INSERT_IGNORE_MASTER, key);
				if(log.isTraceEnabled()){
					log.trace(String.format(CREATED_MASTER, key));
				}
				return null;
			}
		});
	}

}
