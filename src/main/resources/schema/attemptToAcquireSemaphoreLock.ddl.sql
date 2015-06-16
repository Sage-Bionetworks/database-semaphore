CREATE PROCEDURE attemptToAcquireSemaphoreLock(IN lockKey VARCHAR(256), IN timeoutSec INT(4), IN maxLockCount INT(4))
BEGIN
	DECLARE lockKeyExists VARCHAR(256);
	DECLARE countOutstanding INT(4);
	DECLARE newToken VARCHAR(256);
	START TRANSACTION;
	/* Acquire an exclusive lock on the master row.*/
	SELECT LOCK_KEY INTO lockKeyExists FROM SEMAPHORE_MASTER WHERE LOCK_KEY = lockKey FOR UPDATE;

	/*	If the master does not exist then we need to create it in a new transaction */
    IF lockKeyExists IS NULL THEN
		/* To avoid deadlock the insert into master must be done in a separate transaction. */
		COMMIT;
		START TRANSACTION;
		/* Create the lock key in the master table since it does not exist*/
		INSERT IGNORE INTO SEMAPHORE_MASTER (LOCK_KEY) VALUES (lockKey);
		COMMIT;
		START TRANSACTION;
		SELECT LOCK_KEY INTO lockKeyExists FROM SEMAPHORE_MASTER WHERE LOCK_KEY = lockKey FOR UPDATE;
    END IF;
	/* Delete expired locks*/
	SET SQL_SAFE_UPDATES = 0;
	DELETE FROM SEMAPHORE_LOCK WHERE LOCK_KEY = lockKey AND EXPIRES_ON < current_timestamp;
	/* Count outstanding locks*/ 
	SELECT COUNT(*) INTO countOutstanding FROM SEMAPHORE_LOCK WHERE LOCK_KEY = lockKey;
	
	IF countOutstanding < maxLockCount THEN
		SET newToken = UUID();
		INSERT INTO SEMAPHORE_LOCK (LOCK_KEY, TOKEN, EXPIRES_ON) VALUES (lockKey, newToken, (CURRENT_TIMESTAMP + INTERVAL timeoutSec SECOND));
	ELSE
		SET newToken = NULL;
	END IF;
	COMMIT;
	/* push the token to the result set*/
	SELECT newToken AS TOKEN;
END;