CREATE PROCEDURE attemptToAcquireWriteLockPrecursor(IN lockKey VARCHAR(256), IN timeoutSec INT(4))
BEGIN

	DECLARE writeLockToken VARCHAR(256);
	DECLARE precursorToken VARCHAR(256);

	DECLARE newToken VARCHAR(256);
	DECLARE newExpiresOn TIMESTAMP;
	
	/* Lock on master and get the state of the write an precursor tokens */
	CALL lockOnWriteReadMaster(lockKey, writeLockToken, precursorToken);

	/* A precursor lock can be acquired if there is no write lock or precursor lock */
	IF writeLockToken IS NULL AND precursorToken IS NULL THEN
		SET newToken = UUID();
		SET newExpiresOn = CURRENT_TIMESTAMP + INTERVAL timeoutSec SECOND;
		UPDATE WRITE_READ_MASTER SET PRECURSOR_TOKEN = newToken, EXPIRES_ON = newExpiresOn WHERE LOCK_KEY = lockKey;
	ELSE
		SET newToken = NULL;
	END IF;
	COMMIT;
	/* push the token to the result set*/
	SELECT newToken AS TOKEN;
END;