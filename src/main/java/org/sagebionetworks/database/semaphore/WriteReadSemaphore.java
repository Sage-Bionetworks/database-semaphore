package org.sagebionetworks.database.semaphore;

/**
 * An abstraction for a Semaphore that issues either exclusive or shared locks.
 * This type of semaphore is useful for scenarios where there can be either a
 * single writer (exclusive) or many readers (shared).
 * 
 * Implementations of this DAO will have the following characteristics:
 * <ul>
 * <li>An existing write-lock (exclusive) means there are no outstanding
 * read-locks (shared) nor are there any other outstanding write-locks. There
 * can be only one write-lock at a time.</li>
 * <li>An existing read-lock means there can be many other outstanding
 * read-locks but no outstanding write-locks.</li>
 * <li>Acquiring a write-lock is a two phase operation:
 * <ul>
 * <li>First, a write-lock-precursor token must be acquired. Once the
 * write-lock-precursor token has been issued, all new read-lock requests will
 * be rejected.</li>
 * <li>The write-lock-precursor token must be used to acquire the actual
 * write-lock. As soon as all outstanding read-locks are release, the write-lock
 * can be issued to the holder of the write-lock-precursor.</li>
 * </ul>
 * </li>
 * </ul>
 * 
 */
public interface WriteReadSemaphore {

	/**
	 * Attempt to acquire a read-lock (shared) on the resource identified by the
	 * passed lock Key.
	 * 
	 * @param lockKey
	 *            Identifies the resource to lock.
	 * @param The
	 *            maximum number of milliseconds that this read-lock will be
	 *            held. If a lock is not release before this timeout it might be
	 *            forcefully released in order to acquire a write-lock.
	 * @return The token of the read-lock is returned when a lock is acquired.
	 *         Returns null if the lock cannot be acquired at this time.
	 */
	public String acquireReadLock(String lockKey, long timeoutMS);

	/**
	 * When a read-lock (shared) is acquired, it is the responsibly of the lock
	 * holder to release the lock using this method.
	 * 
	 * @param lockKey
	 *            Identifies the resource that was locked.
	 * @param token
	 *            The token issued when the read-lock was acquired.
	 * @throws LockReleaseFailedException
	 *             This is thrown when the lock was forcibly revoked because it
	 *             was not release before the requested timeout expired. If this
	 *             occurs, the timeout set when the lock was acquired might be
	 *             too low.
	 */
	public void releaseReadLock(String lockKey, String token)
			throws LockReleaseFailedException;

	/**
	 * <p>
	 * Attempt to acquire the write-lock-precursor on a resource identified by
	 * the passed lock key.
	 * </p>
	 * <p>
	 * In order to acquire an actual write-lock a caller must first acquire the
	 * write-lock-precursor token. Holding the write-lock-precursor token
	 * indicates an intention to acquire the actual write-lock. Once the
	 * write-lock-precursor has been issued, all new read-lock attempts will be
	 * rejected.
	 * </p>
	 * <p>
	 * The holder of the write-lock-precursor must wait for all outstanding
	 * read-locks to either be released normally or forcibly (in the case of
	 * expired locks). The write-lock-precursor has a short timeout (5 seconds).
	 * However, each attempt to acquire the actual write-lock will refresh the
	 * expiration timer of the write-lock-precursor.
	 * </p>
	 * <p>
	 * The write-lock-precursor token returned by this method is a required
	 * parameter to attempt to acquire the actual write-lock using
	 * {@link #acquireExclusiveLock(String, String, long)}. The
	 * write-lock-precursor will be released when the write-lock is released or
	 * if the timeout expires.
	 * </p>
	 * 
	 * @param lockKey
	 *            Identifies the resource to lock.
	 * @param timoutSec
	 *            The number of seconds the lock can be held before it will 
	 *            automatically be release.
	 * @return The write-lock-precursor token. Returns null if the lock cannot
	 *         be acquired at this time.
	 */
	public String acquireWriteLockPrecursor(String lockKey, long timeoutSec);

	/**
	 * <p>
	 * Attempt to acquire the actual write-lock (exclusive) for a given
	 * resources using the write-lock-precursor acquired with
	 * {@link #acquireExclusiveLockPrecursor(String)}.
	 * </p>
	 * Each time this is is called, outstanding read-locks will be checked and
	 * the timeout of the write-lock-precursor will get reset. Once all
	 * read-locks have been released, this call will create the actual-write
	 * lock identified by the returned token.
	 * <p>
	 * It is assumed that this method will be called in a loop and will simply
	 * return null if the actual write-lock cannot be acquire yet.
	 * </p>
	 * 
	 * 
	 * @param lockKey
	 * @param exclusiveLockPrecursorToken
	 *            This token is issued by
	 *            {@link #acquireExclusiveLockPrecursor(String)}
	 * @param timeoutMS
	 *            The number of milliseconds that the caller expects to hold the
	 *            write-lock on this resource. If the write-lock is not release
	 *            before this amount of time has elapsed, the lock could be
	 *            forcibly released and issued to another caller.
	 * @return The token identifying the actual write-lock. It is the
	 *         responsibility of this token holder to release the write-lock
	 *         when finished by calling
	 *         {@link #releaseExclusiveLock(String, String)} before the given
	 *         timeout expires. Will return null if there are outstanding
	 *         read-locks.
	 */
	public String acquireWriteLock(String lockKey,
			String exclusiveLockPrecursorToken, long timeoutMS);

	/**
	 * When a write-lock (exclusive) is acquired, it is the responsibly of the
	 * lock holder to release the lock using this method.
	 * 
	 * @param lockKey
	 *            Identifies the resource that was locked.
	 * @param token
	 *            The token issued when the write-lock was acquired.
	 * @throws LockReleaseFailedException
	 *             This is thrown when the lock was forcibly revoked because it
	 *             was not release before the requested timeout expired. If this
	 *             occurs, the timeout set when the lock was acquired might be
	 *             too low.
	 */
	public void releaseWriteLock(String lockKey, String token)
			throws LockReleaseFailedException;

	/**
	 * Force the release of all locks. This should not be used under normal
	 * circumstances.
	 */
	public void releaseAllLocks();


}
