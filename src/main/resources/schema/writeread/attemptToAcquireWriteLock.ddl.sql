CREATE PROCEDURE attemptToAcquireWriteLock(IN lockKey VARCHAR(256), IN inPrecursorToken VARCHAR(256), IN timeoutSec INT(4))
BEGIN

	DECLARE writeLockToken VARCHAR(256);
	DECLARE precursorToken VARCHAR(256);
	
	DECLARE countReadLocks INT(4);
	DECLARE newToken VARCHAR(256);
	DECLARE newExpiresOn TIMESTAMP;
	
	DECLARE expiredResults VARCHAR(256);
	/* Used to indicate the passed precursor token is expired*/
	SET expiredResults = 'EXPIRED';
	
	/* Lock on master and get the state of the write an precursor tokens */
	CALL lockOnWriteReadMaster(lockKey, writeLockToken, precursorToken);
	
	/* Remove expired Read locks */
	DELETE FROM WRITE_READ_LOCK WHERE LOCK_KEY = lockKey AND EXPIRES_ON < CURRENT_TIMESTAMP;
	
	/* Count the number of remaining read locks */
	SELECT COUNT(*) INTO countReadLocks FROM WRITE_READ_LOCK WHERE LOCK_KEY = lockKey;
	
	IF writeLockToken IS NOT NULL THEN
		/* another write token has been issued so the passed precursor is expired. */
		SET newToken = expiredResults;
	ELSEIF precursorToken IS NULL OR inPrecursorToken <> precursorToken THEN
		/* The passed precursor does not match the existing so it is expired */
		SET newToken = expiredResults;
	ELSEIF countReadLocks < 1 THEN
		/*  The precursor matches and there are no outstanding read locks so a write lock can be issued.*/
		SET newToken = UUID();
		SET newExpiresOn = CURRENT_TIMESTAMP + INTERVAL timeoutSec SECOND;
		UPDATE WRITE_READ_MASTER SET WRITE_LOCK_TOKEN = newToken, EXPIRES_ON = newExpiresOn WHERE LOCK_KEY = lockKey;
	ELSE
		/* Precursor matches but there are outstanding read locks so a write lock cannot be issued at this time. */
		SET newToken = NULL;
		/* Refresh the precursor expires time.*/
		SET newExpiresOn = CURRENT_TIMESTAMP + INTERVAL timeoutSec SECOND;
		UPDATE WRITE_READ_MASTER SET EXPIRES_ON = newExpiresOn WHERE LOCK_KEY = lockKey;
	END IF;
	
	COMMIT;
	/* push the token to the result set*/
	SELECT newToken AS TOKEN;
END;