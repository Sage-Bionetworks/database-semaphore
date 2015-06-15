CREATE PROCEDURE refreshSemaphoreLock(IN lockKey VARCHAR(256), IN tokenIn VARCHAR(256), IN timeoutSec INT(4))
BEGIN
	DECLARE lockKeyExists VARCHAR(256);
	/*Use READ COMMITTED to prevent the use of gap locks.*/
	SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
	START TRANSACTION;
	/* acquire an exclusive lock on the master row.*/
	SELECT LOCK_KEY INTO lockKeyExists FROM SEMAPHORE_MASTER WHERE LOCK_KEY = lockKey FOR UPDATE;

	/*	If the master does not exist then we need to create it in a new transaction */
    IF lockKeyExists IS NULL THEN
		/* Master lock does not exist */
		SELECT -1 AS RESULT;
	ELSE
		SET SQL_SAFE_UPDATES = 0;
		UPDATE SEMAPHORE_LOCK SET EXPIRES_ON = (CURRENT_TIMESTAMP + INTERVAL timeoutSec SECOND) WHERE LOCK_KEY = lockKey AND TOKEN = tokenIn;
		/*Count the rows affected by the delete*/
		SELECT ROW_COUNT() AS RESULT;
		SET SQL_SAFE_UPDATES = 1;
    END IF;
	COMMIT;
END;