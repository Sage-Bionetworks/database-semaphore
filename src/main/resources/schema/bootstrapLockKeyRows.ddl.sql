CREATE PROCEDURE bootstrapLockKeyRows(IN lockKey VARCHAR(256), IN maxLockCount INT(4))
    MODIFIES SQL DATA
    SQL SECURITY INVOKER
BEGIN
    DECLARE nextNumber TINYINT;
    DECLARE lockCount TINYINT;
	  
    /* Ensure the correct number of lock rows exist. */ 
    SELECT COUNT(LOCK_NUM) INTO lockCount FROM SEMAPHORE_LOCK WHERE LOCK_KEY = lockKey;
    IF lockCount < maxLockCount THEN
    	/* Unconditionally add all lock rows for this key.  See PLFM-5909. */
    	SET nextNumber = 0;
    	WHILE nextNumber < maxLockCount DO
			INSERT IGNORE INTO SEMAPHORE_LOCK (LOCK_KEY, LOCK_NUM, TOKEN, EXPIRES_ON) VALUES (lockKey, nextNumber, NULL, NULL);
			SET nextNumber = nextNumber + 1;
		END WHILE;
	END IF;

END;