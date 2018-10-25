package org.sagebionetworks.database.semaphore;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * This is a database level integration test for the WriteReadSemaphoreImpl.
 * In order to run this test you will need ensure the following system properties are set:
 * "-Djdbc.url=jdbc:mysql://localhost/semaphore"
 * "-Djdbc.username=your_username"
 * "-Djdbc.password=your_password"
 * 
 * To run in eclipse make sure the above properties are added to the "VM Argumetns" of the test.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:test-context.spb.xml" })
public class WriteReadSemaphoreImplTest {
	
	
	@Autowired
	WriteReadSemaphore writeReadsemaphore;
	
	String key;
	int lockTimeoutSec;
	long sleepTillExpiredMS;
	
	@Before
	public void before(){
		// release all locks
		writeReadsemaphore.releaseAllLocks();
		
		key = "123";
		lockTimeoutSec = 10;
		sleepTillExpiredMS = 1000*((long)lockTimeoutSec+1L);
	}
	
	@After
	public void after(){
		// release all locks
		writeReadsemaphore.releaseAllLocks();
	}
	
	@Test
	public void testHappyReadLock(){
		long start = System.currentTimeMillis();
		// Should be able to get a read lock
		String token = writeReadsemaphore.acquireReadLock(key, lockTimeoutSec);
		assertNotNull(token);
		// We should be able to release the lock
		writeReadsemaphore.releaseReadLock(key, token);
		System.out.println("Read lock timing: "+(System.currentTimeMillis()-start));
	}
	
	@Test
	public void testHappyWriteLock(){
		long start = System.currentTimeMillis();
		// First get the lock-precursor token
		String precursorToken = writeReadsemaphore.acquireWriteLockPrecursor(key, lockTimeoutSec);
		assertNotNull(precursorToken);
		// Use it to get the actual token
		String lockToken = writeReadsemaphore.acquireWriteLock(key, precursorToken, lockTimeoutSec);
		assertNotNull(lockToken);
		// We should be able to release the lock
		writeReadsemaphore.releaseWriteLock(key, lockToken);
		System.out.println("Write lock timing: "+(System.currentTimeMillis()-start));
		
		// We should now be able to get the lock again
		// First get the lock-precursor token
		precursorToken = writeReadsemaphore.acquireWriteLockPrecursor(key, lockTimeoutSec);
		assertNotNull(precursorToken);
		// Use it to get the actual token
		lockToken = writeReadsemaphore.acquireWriteLock(key, precursorToken, lockTimeoutSec);
		// We should be able to release the lock
		writeReadsemaphore.releaseWriteLock(key, lockToken);
	}
	
	@Test
	public void testAcquireReadLockWithOutstandingWritePrecursor() throws InterruptedException{
		// first get a read lock.
		// Should be able to get a read lock
		String readLockToken = writeReadsemaphore.acquireReadLock(key, lockTimeoutSec);
		assertNotNull(readLockToken);
		// Now acquire the write-lock-precursor
		String writeLockPrecursor = writeReadsemaphore.acquireWriteLockPrecursor(key, lockTimeoutSec);
		assertNotNull(writeLockPrecursor);
		// Now we should not be should not be able to get a new read lock
		String lock = writeReadsemaphore.acquireReadLock(key, lockTimeoutSec);
		assertEquals("Attempting to get a new read-lock when there is an outstanding write-lock-precursor should have failed.", null, lock);
		// Now let the precursor expire and try again.
		Thread.sleep(sleepTillExpiredMS);
		// This time it should work
		String readLockTwo = writeReadsemaphore.acquireReadLock(key, lockTimeoutSec);
		assertNotNull(readLockTwo);
	}
	
	@Test (expected=LockExpiredException.class)
	public void testPrecursorExpired() throws InterruptedException{
		// Now acquire the write-lock-precursor
		String writeLockPrecursor = writeReadsemaphore.acquireWriteLockPrecursor(key, lockTimeoutSec);
		assertNotNull(writeLockPrecursor);
		// wait for the token to expire.
		Thread.sleep(sleepTillExpiredMS);
		// The precursor should be expired.
		String lock = writeReadsemaphore.acquireWriteLock(key, writeLockPrecursor, lockTimeoutSec);
		System.out.println(lock);
	}
	
	
	@Test
	public void testAcquireReadLockWithOutstandingWriteLock() throws InterruptedException{
		// first get a read lock.
		// Should be able to get a read lock
		String readLockToken = writeReadsemaphore.acquireReadLock(key, lockTimeoutSec);
		assertNotNull(readLockToken);
		// Now acquire the write-lock-precursor
		String writeLockPrecursor = writeReadsemaphore.acquireWriteLockPrecursor(key, lockTimeoutSec);
		assertNotNull(writeLockPrecursor);
		// Now attempt to acquire the actual write-lock.
		String writeLockToken = writeReadsemaphore.acquireWriteLock(key, writeLockPrecursor, lockTimeoutSec);
		assertEquals("Should not be able to get the actual write-lock when there is an outstanding read-lock",null, writeLockToken);
		// Release the read-lock so we can get the write-lock
		writeReadsemaphore.releaseReadLock(key, readLockToken);
		// Now get the write-lock
		writeLockToken = writeReadsemaphore.acquireWriteLock(key, writeLockPrecursor, lockTimeoutSec);
		assertNotNull("Should have been able to get the actual write-lock as there are no more outstanding read-lock", writeLockToken);
		
		// Now we should not be should not be able to get a new read lock
		String lock = writeReadsemaphore.acquireReadLock(key, lockTimeoutSec);
		assertEquals("Attempting to get a new read-lock when there is an outstanding write-lock should have failed.", null, lock);
		// Now release the write lock and try again
		writeReadsemaphore.releaseWriteLock(key, writeLockToken);
		// This time it should work
		String readLockTwo = writeReadsemaphore.acquireReadLock(key, lockTimeoutSec);
		assertNotNull(readLockTwo);
	}
	
	@Test
	public void testAcquireSecondWriteLockPrecursor() throws InterruptedException{
		// Now acquire the write-lock-precursor
		String writeLockPrecursor = writeReadsemaphore.acquireWriteLockPrecursor(key, lockTimeoutSec);
		assertNotNull(writeLockPrecursor);
		// Trying to get a precursor again should fail
		String lock = writeReadsemaphore.acquireWriteLockPrecursor(key, lockTimeoutSec);
		assertEquals("Attempting to get a second write-lock-precursor should fail when on is already outstanding.", null, lock);
		// Now let the precursor expire and try again.
		Thread.sleep(sleepTillExpiredMS);
		// This time it should work
		writeLockPrecursor = writeReadsemaphore.acquireWriteLockPrecursor(key, lockTimeoutSec);
		assertNotNull(writeLockPrecursor);
	}
	
	@Test
	public void testForcedReadLockRelease() throws InterruptedException{
		// Get two read locks
		String readLockOne = writeReadsemaphore.acquireReadLock(key, lockTimeoutSec);
		assertNotNull(readLockOne);
		String readLockTwo = writeReadsemaphore.acquireReadLock(key, lockTimeoutSec);
		assertNotNull(readLockTwo);
		// Get the precursor
		String writeLockPrecursor = writeReadsemaphore.acquireWriteLockPrecursor(key, lockTimeoutSec);
		assertNotNull(writeLockPrecursor);
		long start = System.currentTimeMillis();
		String writeLock = null;
		do{
			// try to get writeLock
			writeLock = writeReadsemaphore.acquireWriteLock(key, writeLockPrecursor, lockTimeoutSec);
			assertTrue("Timed-out waiting for read-locks to expire", (System.currentTimeMillis()-start) < sleepTillExpiredMS*2);
			if(writeLock == null){
				System.out.println("Waiting for read-locks to expire...");
				Thread.sleep(1000);
			}
		}while(writeLock == null);
		// We should now have the write lock
		assertNotNull(writeLock);
	}
	
	@Test
	public void testForcedWriteLockRelease() throws InterruptedException{

		// First acquire a precursor
		String writeLockPrecursor = writeReadsemaphore.acquireWriteLockPrecursor(key, lockTimeoutSec);
		assertNotNull(writeLockPrecursor);
		String writeLock = writeReadsemaphore.acquireWriteLock(key, writeLockPrecursor, lockTimeoutSec);
		assertNotNull(writeLock);
		long start = System.currentTimeMillis();
		String readLock = null;
		do{
			// try to get read lock
			readLock = writeReadsemaphore.acquireReadLock(key, lockTimeoutSec);
			assertTrue("Timed-out waiting for write-locks to expire", (System.currentTimeMillis()-start) < sleepTillExpiredMS*2);
			if(readLock == null){
				System.out.println("Waiting for write-locks to expire...");
				Thread.sleep(1000);
			}
		}while(readLock == null);
		// We should now have the write lock
		assertNotNull(readLock);
	}
	
	@Test
	public void testRefreshReadLock() throws Exception{
		String token = writeReadsemaphore.acquireReadLock(key, lockTimeoutSec);
		// Keep refreshing the lock
		for(int i=0; i<4; i++){
			writeReadsemaphore.refreshReadLock(key, token, lockTimeoutSec);
			Thread.sleep(1000);
		}
		// 
		String writeLockPrecursor = writeReadsemaphore.acquireWriteLockPrecursor(key, lockTimeoutSec);
		assertNotNull(writeLockPrecursor);
		String writeLock = writeReadsemaphore.acquireWriteLock(key, writeLockPrecursor, lockTimeoutSec);
		assertEquals(null, writeLock);
		// The total time should now have exceeded the original lock timeout.
		writeReadsemaphore.releaseReadLock(key, token);
	}
	
	@Test (expected=LockExpiredException.class)
	public void testRefreshReadLockExpired() throws Exception{
		String token = writeReadsemaphore.acquireReadLock(key, lockTimeoutSec);
		// Wait past the expiration 
		Thread.sleep(sleepTillExpiredMS);
		// Get the precursor token now that it is expired
		String precurstor = writeReadsemaphore.acquireWriteLockPrecursor(key, lockTimeoutSec);
		assertNotNull(precurstor);
		String writeToken = writeReadsemaphore.acquireWriteLock(key, precurstor, lockTimeoutSec);
		assertNotNull(writeToken);
		// This should fail since the lock was not refreshed before it expired.
		writeReadsemaphore.refreshReadLock(key, token, lockTimeoutSec);
	}
	
	@Test
	public void testRefreshWriteLock() throws Exception{
		String precurstor = writeReadsemaphore.acquireWriteLockPrecursor(key, lockTimeoutSec);
		assertNotNull(precurstor);
		String token = writeReadsemaphore.acquireWriteLock(key,precurstor, lockTimeoutSec);
		// Keep refreshing the lock
		for(int i=0; i<4; i++){
			writeReadsemaphore.refreshWriteLock(key, token, lockTimeoutSec);
			Thread.sleep(1000);
		}
		// If the refresh did not work then a read lock could be issued.
		String readLock  = writeReadsemaphore.acquireReadLock(key, lockTimeoutSec);
		assertEquals(null, readLock);
		// The total time should now have exceeded the original lock timeout.
		writeReadsemaphore.releaseWriteLock(key, token);
	}
	
	@Test (expected=LockExpiredException.class)
	public void testRefreshWriteLockExpired() throws Exception{
		String precurstor = writeReadsemaphore.acquireWriteLockPrecursor(key, lockTimeoutSec);
		assertNotNull(precurstor);
		String token = writeReadsemaphore.acquireWriteLock(key,precurstor, lockTimeoutSec);
		// Wait past the expiration 
		Thread.sleep(sleepTillExpiredMS);
		// Get the read token now that it is expired
		String readLock = writeReadsemaphore.acquireReadLock(key, lockTimeoutSec);
		assertNotNull(readLock);
		// This should fail since the lock was not refreshed before it expired.
		writeReadsemaphore.refreshWriteLock(key, token, lockTimeoutSec);
	}
	
	@Test
	public void testMultipleWriteLocks() throws Exception{
		String key1 = "456";
		String key2 = "567";
		// acquire both locks
		String preToken1 = writeReadsemaphore.acquireWriteLockPrecursor(key1, lockTimeoutSec);
		String preToken2 = writeReadsemaphore.acquireWriteLockPrecursor(key2, lockTimeoutSec);
		// Should not be able to get reads on either
		validateCannotGetRead(1, key1, key2);
		
		// get both write locks
		String writeToken1 = writeReadsemaphore.acquireWriteLock(key1, preToken1, lockTimeoutSec);
		assertNotNull(writeToken1);
		String writeToken2 = writeReadsemaphore.acquireWriteLock(key2, preToken2, lockTimeoutSec);
		assertNotNull(writeToken2);
		assertFalse(writeToken1.equals(writeToken2));
		// Refresh both locks
		writeReadsemaphore.refreshWriteLock(key1, writeToken1, lockTimeoutSec);
		writeReadsemaphore.refreshWriteLock(key2, writeToken2, lockTimeoutSec);
		// Should not be able to get reads on either
		validateCannotGetRead(1, key1, key2);
		// Should be able to release both locks
		writeReadsemaphore.releaseWriteLock(key1, writeToken1);
		writeReadsemaphore.releaseWriteLock(key2, writeToken2);
	}
	
	@Test
	public void testMultipleReadLocks() throws Exception {
		String key1 = "456";
		String key2 = "567";
		String readToken1 = writeReadsemaphore.acquireReadLock(key1, lockTimeoutSec);
		assertNotNull(readToken1);
		String readToken2 = writeReadsemaphore.acquireReadLock(key2, lockTimeoutSec);
		assertNotNull(readToken2);
		// should be able to refresh both
		writeReadsemaphore.refreshReadLock(key1, readToken1, lockTimeoutSec);
		writeReadsemaphore.refreshReadLock(key2, readToken2, lockTimeoutSec);
		// Should be able to release both
		writeReadsemaphore.releaseReadLock(key1, readToken1);
		writeReadsemaphore.releaseReadLock(key2, readToken2);
	}
	
	/**
	 * Helper to validate read locks cannot be aquired for the given keys.
	 * @param timoutSec
	 * @param keys
	 */
	private void validateCannotGetRead(int timoutSec, String...keys){
		for(String key: keys){
			String token = writeReadsemaphore.acquireReadLock(key, timoutSec);
			assertEquals("Should not have been able to get a read locks but recieved token: "+token, null, token);
		}
	}

}
