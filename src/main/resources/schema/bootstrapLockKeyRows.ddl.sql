CREATE PROCEDURE bootstrapLockKeyRows(IN lockKey VARCHAR(256), IN maxLockCount INT(4))
    MODIFIES SQL DATA
    SQL SECURITY INVOKER
BEGIN
    DECLARE nextNumber TINYINT;
    DECLARE lockCount TINYINT;
	  
    /* Ensure the correct number of lock rows exist. */ 
    SELECT COUNT(LOCK_NUM) INTO lockCount FROM SEMAPHORE_LOCK WHERE LOCK_KEY = lockKey;
    IF lockCount < maxLockCount THEN
    	/* If two separate transactions attempt to bootstrap the same rows at the same time, the first transaction will
    	 * block the second transaction due to the exclusive locks used on an insert.  This means a slow-down in the
    	 *  first transaction will spread to all other transactions. See: PLFM-8236.
    	 *  By enabling autocommit we can minimize the amount of time a single insert can block other transactions.
    	 * Note: This must be called from a new session, as enabling autocommit will trigger the commit of any outstanding
    	 * transactions on the same session.  */
    	SET autocommit=1;
    	/* Unconditionally add all lock rows for this key.  See PLFM-5909. */
    	SET nextNumber = 0;
    	WHILE nextNumber < maxLockCount DO
			INSERT IGNORE INTO SEMAPHORE_LOCK (LOCK_KEY, LOCK_NUM, TOKEN, EXPIRES_ON) VALUES (lockKey, nextNumber, NULL, NULL);
			SET nextNumber = nextNumber + 1;
		END WHILE;
		/* Since we enabled autocommit before the inserts, we must restore to the disabled state. */
		SET autocommit=0;
	END IF;

END;