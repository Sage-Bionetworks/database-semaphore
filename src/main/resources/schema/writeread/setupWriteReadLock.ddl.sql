CREATE PROCEDURE setupWriteReadLock(IN lockKey VARCHAR(256))
BEGIN
	/* Create the lock key in the master table*/
	START TRANSACTION;
	INSERT IGNORE INTO WRITE_READ_MASTER (LOCK_KEY) VALUES (lockKey);
	COMMIT;
END;