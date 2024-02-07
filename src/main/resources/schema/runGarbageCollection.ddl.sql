CREATE PROCEDURE runGarbageCollection()
    MODIFIES SQL DATA
    SQL SECURITY INVOKER
BEGIN

	DECLARE rowId MEDIUMINT DEFAULT NULL;

	the_loop: LOOP
		/* 
		 * Find the first row that can be deleted without blocking.
		 * Start a transaction within the loop the minimize the length of the transaction.
		 */
	   	START TRANSACTION;
    	SELECT ROW_ID INTO rowId FROM SEMAPHORE_LOCK WHERE TOKEN IS NULL AND
			(now() > EXPIRES_ON || EXPIRES_ON IS NULL) LIMIT 1 FOR UPDATE SKIP LOCKED;
		
		IF rowId IS NOT NULL THEN
			DELETE FROM SEMAPHORE_LOCK WHERE ROW_ID = rowId;
			COMMIT;
			SET rowId = NULL;
		ELSE
			/* 
			 * unable to find any more rows to delete so garbage collection is done.
			 */
			COMMIT;
			LEAVE the_loop;
		END IF;
	END LOOP the_loop;
END;