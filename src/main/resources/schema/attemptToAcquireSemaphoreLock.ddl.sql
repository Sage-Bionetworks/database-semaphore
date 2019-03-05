CREATE PROCEDURE attemptToAcquireSemaphoreLock(IN lockKey VARCHAR(256), IN timeoutSec INT(4), IN maxLockCount INT(4))
    MODIFIES SQL DATA
    SQL SECURITY INVOKER
BEGIN
	DECLARE newToken VARCHAR(256) DEFAULT NULL;
    DECLARE maxNumber TINYINT;
	DECLARE freeNumber TINYINT;
	
	SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
    
    /* Ensure a row exists for each number up to the maxLockCount for the given key */ 
    SELECT MAX(LOCK_NUM) INTO maxNumber FROM SEMAPHORE_LOCK WHERE LOCK_KEY = lockKey;
    IF maxNumber IS NULL THEN
		SET maxNumber = 0;
	END IF;
    WHILE maxNumber < maxLockCount DO
		INSERT IGNORE INTO SEMAPHORE_LOCK (LOCK_KEY, LOCK_NUM, TOKEN, EXPIRES_ON) VALUES (lockKey, maxNumber, NULL, NULL);
		SET maxNumber = maxNumber + 1;
	END WHILE;
	
	START TRANSACTION;
	/* Find the first number for the given lock that has a null token or is expired. */
	SELECT LOCK_NUM INTO freeNumber FROM SEMAPHORE_LOCK WHERE LOCK_KEY = lockKey AND LOCK_NUM < maxLockCount
		AND (TOKEN IS NULL OR EXPIRES_ON < current_timestamp) LIMIT 1 FOR UPDATE SKIP LOCKED;
	
    /* Claim this number and issue a token */
	IF freeNumber IS NOT NULL THEN
		SET newToken = UUID();
        UPDATE SEMAPHORE_LOCK SET TOKEN = newToken, EXPIRES_ON = (CURRENT_TIMESTAMP + INTERVAL timeoutSec SECOND)
        	WHERE LOCK_KEY = lockKey AND LOCK_NUM = freeNumber;
	END IF;
	COMMIT;
	/* push the token to the result set*/
	SELECT newToken AS TOKEN;
END;