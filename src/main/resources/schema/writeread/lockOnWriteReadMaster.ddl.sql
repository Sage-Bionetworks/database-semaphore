/* This procedure will start a new transaction and lock on the master row for the given key.  The output writeLockToken and precursorToken will be non-null if either exists and are not expired for the given lock. */
CREATE PROCEDURE lockOnWriteReadMaster(IN lockKey VARCHAR(256), OUT writeLockToken VARCHAR(256), OUT precursorToken VARCHAR(256))
BEGIN
	DECLARE lockKeyExists VARCHAR(256);
	DECLARE writeExpiresOn TIMESTAMP;
	DECLARE semaphoreLock VARCHAR(256);
	
	START TRANSACTION;
	/* As a temporary fix for PLFM-5451 to prevent deadlock all updates are gated by this single row lock */
	SELECT LOCK_KEY INTO semaphoreLock FROM WRITE_READ_MASTER WHERE LOCK_KEY = 'SEMAPHORE_LOCK_ALL_ROWS'  FOR UPDATE;
	
	IF semaphoreLock IS NULL THEN
		INSERT IGNORE INTO WRITE_READ_MASTER (LOCK_KEY) VALUES ('SEMAPHORE_LOCK_ALL_ROWS');
	END IF;


	/* Lock on the master row. This will result in a gap lock if the row does not exit.*/
	SELECT 
			LOCK_KEY, WRITE_LOCK_TOKEN, PRECURSOR_TOKEN, EXPIRES_ON
		INTO lockKeyExists , writeLockToken , precursorToken , writeExpiresOn FROM
			WRITE_READ_MASTER
		WHERE
			LOCK_KEY = lockKey;
	
	/* If the lock key does not exist we need to create it in a new transaction to prevent deadlock */
	IF lockKeyExists IS NULL THEN
		INSERT IGNORE INTO WRITE_READ_MASTER (LOCK_KEY) VALUES (lockKey);
		/* Lock on the master */
		SELECT 
			LOCK_KEY, WRITE_LOCK_TOKEN, PRECURSOR_TOKEN, EXPIRES_ON
		INTO lockKeyExists , writeLockToken , precursorToken , writeExpiresOn FROM
			WRITE_READ_MASTER
		WHERE
			LOCK_KEY = lockKey;
	END IF;

	/* Is the write lock expired */
	IF writeExpiresOn IS NOT NULL AND writeExpiresOn < CURRENT_TIMESTAMP THEN
		/* The lock is expired so release it. */
		UPDATE WRITE_READ_MASTER SET WRITE_LOCK_TOKEN = NULL, PRECURSOR_TOKEN = NULL, EXPIRES_ON = NULL WHERE
			LOCK_KEY = lockKey;
		/* Clear lock local variables*/
		SET writeLockToken = NULL;
		SET precursorToken = NULL;
		SET writeExpiresOn = NULL;
	END IF;

END;