CREATE PROCEDURE releaseSemaphoreLock(IN tokenIn VARCHAR(256))
    MODIFIES SQL DATA
    SQL SECURITY INVOKER
BEGIN
	/* We start a new transaction here even though the caller has started a new transaction.
     * If the caller's thread is held up for any reason, the caller's transaction might
     * remain open for a long period of time.  By starting a new transaction here, we can 
     * ensure that any slow down from a caller does not spread to other transactions that
     * might be attempting to get the same exclusive lock.
     * Note: The caller must make this call from a new database session (i.e. using Propagation.REQUIRES_NEW),
     * as the start transaction call here will automatically commit any outstanding transactions on the
     * same session.  See: PLFM-8236.
     */
    START TRANSACTION;
	UPDATE SEMAPHORE_LOCK SET TOKEN = NULL, CONTEXT = NULL WHERE TOKEN = tokenIn;
	SELECT ROW_COUNT() AS RESULT;
	COMMIT;
END