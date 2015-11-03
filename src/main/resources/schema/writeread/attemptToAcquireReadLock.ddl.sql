CREATE PROCEDURE attemptToAcquireReadLock(IN lockKey VARCHAR(256), IN timeoutSec INT(4))
BEGIN
	DECLARE lockKeyExists VARCHAR(256);
	DECLARE writeLockToken VARCHAR(256);
	DECLARE precursorToken VARCHAR(256);
	DECLARE writeExpire TIMESTAMP;
	DECLARE countOutstanding INT(4);
	DECLARE newToken VARCHAR(256);
	START TRANSACTION;
	/* Acquire an exclusive lock on the master row.*/
	SELECT 
			LOCK_KEY, WRITE_LOCK_TOKEN, PRECURSOR_TOKEN, EXPIRES_ON
		INTO lockKeyExists , writeLockToken , precursorToken , writeExpire FROM
			WRITE_READ_MASTER
		WHERE
			LOCK_KEY = lockKey
		FOR UPDATE;

	/* Is the write lock expired */
	IF writeExpire IS NOT NULL AND writeExpire > CURRENT_TIMESTAMP THEN
		/* The lock is expired so release it. */
		UPDATE WRITE_READ_MASTER SET WRITE_LOCK_TOKEN = NULL, PRECURSOR_TOKEN = NULL, EXPIRES_ON = NULL WHERE
			LOCK_KEY = lockKey;
		/* Clear lock vars*/
		SET writeLockToken = NULL;
		SET precursorToken = NULL;
		SET writeExpire = NULL;
	END IF;

	/* A read lock can be acquired if there is no write lock or precursor lock */
	IF lockKeyExits IS NOT NULL AND writeLockToken IS NULL AND precursorToken IS NULL THEN
		SET newToken = UUID();
		INSERT INTO WRITE_READ_LOCK (LOCK_KEY, TOKEN, EXPIRES_ON) VALUES (lockKey, newToken, (CURRENT_TIMESTAMP + INTERVAL timeoutSec SECOND));
	ELSE
		SET newToken = NULL;
	END IF;
	COMMIT;
	/* push the token to the result set*/
	SELECT newToken AS TOKEN;
END;