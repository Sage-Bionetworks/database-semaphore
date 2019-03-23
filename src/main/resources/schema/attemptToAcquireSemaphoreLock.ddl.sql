CREATE PROCEDURE attemptToAcquireSemaphoreLock(IN lockKey VARCHAR(256), IN timeoutSec INT(4), IN maxLockCount INT(4))
    MODIFIES SQL DATA
    SQL SECURITY INVOKER
BEGIN
	DECLARE newToken VARCHAR(256) DEFAULT NULL;
    DECLARE nextNumber TINYINT;
	DECLARE rowId MEDIUMINT;
	
	SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
    
    /* Ensure a row exists for each number up to the maxLockCount for the given key */ 
    SELECT MAX(LOCK_NUM) INTO nextNumber FROM SEMAPHORE_LOCK WHERE LOCK_KEY = lockKey;
    IF nextNumber IS NULL THEN
		SET nextNumber = 0;
	ELSE
		SET nextNumber = nextNumber + 1;
	END IF;
    WHILE nextNumber < maxLockCount DO
		INSERT IGNORE INTO SEMAPHORE_LOCK (LOCK_KEY, LOCK_NUM, TOKEN, EXPIRES_ON) VALUES (lockKey, nextNumber, NULL, NULL);
		SET nextNumber = nextNumber + 1;
	END WHILE;
	
	START TRANSACTION;
	/* Find the first number for the given lock that has a null token or is expired. */
	SELECT ROW_ID INTO rowId FROM SEMAPHORE_LOCK WHERE LOCK_KEY = lockKey AND LOCK_NUM < maxLockCount
		AND (TOKEN IS NULL OR EXPIRES_ON < current_timestamp) LIMIT 1 FOR UPDATE SKIP LOCKED;
	
    /* Claim this number and issue a token */
	IF rowId IS NOT NULL THEN
		SET newToken = UUID();
        UPDATE SEMAPHORE_LOCK SET TOKEN = newToken, EXPIRES_ON = (CURRENT_TIMESTAMP + INTERVAL timeoutSec SECOND)
        	WHERE ROW_ID = rowId;
	END IF;
	COMMIT;
	/* push the token to the result set*/
	SELECT newToken AS TOKEN;
END;