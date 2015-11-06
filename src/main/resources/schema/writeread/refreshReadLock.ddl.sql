CREATE PROCEDURE refreshReadLock(IN lockKey VARCHAR(256), IN tokenIn VARCHAR(256), IN timeoutSec INT(4))
BEGIN
	DECLARE writeLockToken VARCHAR(256);
	DECLARE precursorToken VARCHAR(256);
	DECLARE newExpiresOn TIMESTAMP;

	/* Lock on master and get the state of the write an precursor tokens */
	CALL lockOnWriteReadMaster(lockKey, writeLockToken, precursorToken);
	/* refresh the lock */
	SET newExpiresOn = CURRENT_TIMESTAMP + INTERVAL timeoutSec SECOND;
	UPDATE WRITE_READ_LOCK SET EXPIRES_ON = newExpiresOn WHERE LOCK_KEY = lockKey AND TOKEN = tokenIn;
	/*Count the rows affected by the update*/
	SELECT ROW_COUNT() AS RESULT;
	COMMIT;
END;