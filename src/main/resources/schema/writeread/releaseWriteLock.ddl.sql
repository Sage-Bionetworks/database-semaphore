CREATE PROCEDURE releaseWriteLock(IN lockKey VARCHAR(256), IN tokenIn VARCHAR(256))
BEGIN
	DECLARE writeLockToken VARCHAR(256);
	DECLARE precursorToken VARCHAR(256);

	/* Lock on master and get the state of the write an precursor tokens */
	CALL lockOnWriteReadMaster(lockKey, writeLockToken, precursorToken);
	
	/* Clear the write lock using the passed token*/
	UPDATE WRITE_READ_MASTER SET WRITE_LOCK_TOKEN = NULL, PRECURSOR_TOKEN = NULL, EXPIRES_ON = NULL WHERE LOCK_KEY = lockKey AND WRITE_LOCK_TOKEN = tokenIn;
	/*Count the rows affected by the delete*/
	SELECT ROW_COUNT() AS RESULT;
	COMMIT;
END;