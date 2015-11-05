package org.sagebionetworks.database.semaphore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * This is a database level integration test for the CountingSemaphore.
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
	
	int WRITE_LOCK_PRECURSOR_TIMEOUT_SEC = 5;
	
	@Autowired
	WriteReadSemaphore writeReadsemaphore;
	
	String key;
	
	@Before
	public void before(){
		// release all locks
		writeReadsemaphore.releaseAllLocks();
		
		key = "123";
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
		String token = writeReadsemaphore.acquireReadLock(key, 1);
		assertNotNull(token);
		// We should be able to release the lock
		writeReadsemaphore.releaseReadLock(key, token);
		System.out.println("Read lock timing: "+(System.currentTimeMillis()-start));
	}
	
	@Test
	public void testHappyWriteLock(){
		long start = System.currentTimeMillis();
		// First get the lock-precursor token
		String precursorToken = writeReadsemaphore.acquireWriteLockPrecursor(key, WRITE_LOCK_PRECURSOR_TIMEOUT_SEC);
		assertNotNull(precursorToken);
		// Use it to get the actual token
		String lockToken = writeReadsemaphore.acquireWriteLock(key, precursorToken, 1);
		assertNotNull(lockToken);
		// We should be able to release the lock
		writeReadsemaphore.releaseWriteLock(key, lockToken);
		System.out.println("Write lock timing: "+(System.currentTimeMillis()-start));
		
		// We should now be able to get the lock again
		// First get the lock-precursor token
		precursorToken = writeReadsemaphore.acquireWriteLockPrecursor(key, WRITE_LOCK_PRECURSOR_TIMEOUT_SEC);
		assertNotNull(precursorToken);
		// Use it to get the actual token
		lockToken = writeReadsemaphore.acquireWriteLock(key, precursorToken, 1);
		// We should be able to release the lock
		writeReadsemaphore.releaseWriteLock(key, lockToken);
	}
	
	@Test
	public void testAcquireReadLockWithOutstandingWritePrecursor() throws InterruptedException{
		// first get a read lock.
		// Should be able to get a read lock
		String readLockToken = writeReadsemaphore.acquireReadLock(key, 1);
		assertNotNull(readLockToken);
		// Now acquire the write-lock-precursor
		String writeLockPrecursor = writeReadsemaphore.acquireWriteLockPrecursor(key, WRITE_LOCK_PRECURSOR_TIMEOUT_SEC);
		assertNotNull(writeLockPrecursor);
		// Now we should not be should not be able to get a new read lock
		String lock = writeReadsemaphore.acquireReadLock(key, 1);
		assertEquals("Attempting to get a new read-lock when there is an outstanding write-lock-precursor should have failed.", null, lock);
		// Now let the precursor expire and try again.
		Thread.sleep(1000*(WRITE_LOCK_PRECURSOR_TIMEOUT_SEC+2));
		// This time it should work
		String readLockTwo = writeReadsemaphore.acquireReadLock(key, 1);
		assertNotNull(readLockTwo);
	}
	
	@Test
	public void testAcquireReadLockWithOutstandingWriteLock() throws InterruptedException{
		// first get a read lock.
		// Should be able to get a read lock
		String readLockToken = writeReadsemaphore.acquireReadLock(key, 1);
		assertNotNull(readLockToken);
		// Now acquire the write-lock-precursor
		String writeLockPrecursor = writeReadsemaphore.acquireWriteLockPrecursor(key, WRITE_LOCK_PRECURSOR_TIMEOUT_SEC);
		assertNotNull(writeLockPrecursor);
		// Now attempt to acquire the actual write-lock.
		String writeLockToken = writeReadsemaphore.acquireWriteLock(key, writeLockPrecursor, 1);
		assertEquals("Should not be able to get the actual write-lock when there is an outstanding read-lock",null, writeLockToken);
		// Release the read-lock so we can get the write-lock
		writeReadsemaphore.releaseReadLock(key, readLockToken);
		// Now get the write-lock
		writeLockToken = writeReadsemaphore.acquireWriteLock(key, writeLockPrecursor, 1);
		assertNotNull("Should have been able to get the actual write-lock as there are no more outstanding read-lock", writeLockToken);
		
		// Now we should not be should not be able to get a new read lock
		String lock = writeReadsemaphore.acquireReadLock(key, 1);
		assertEquals("Attempting to get a new read-lock when there is an outstanding write-lock should have failed.", null, lock);
		// Now release the write lock and try again
		writeReadsemaphore.releaseWriteLock(key, writeLockToken);
		// This time it should work
		String readLockTwo = writeReadsemaphore.acquireReadLock(key, 1);
		assertNotNull(readLockTwo);
	}
	
	@Test
	public void testAcquireSecondWriteLockPrecursor() throws InterruptedException{
		// Now acquire the write-lock-precursor
		String writeLockPrecursor = writeReadsemaphore.acquireWriteLockPrecursor(key, WRITE_LOCK_PRECURSOR_TIMEOUT_SEC);
		assertNotNull(writeLockPrecursor);
		// Trying to get a precursor again should fail
		String lock = writeReadsemaphore.acquireWriteLockPrecursor(key, WRITE_LOCK_PRECURSOR_TIMEOUT_SEC);
		assertEquals("Attempting to get a second write-lock-precursor should fail when on is already outstanding.", null, lock);
		// Now let the precursor expire and try again.
		Thread.sleep(1000*(WRITE_LOCK_PRECURSOR_TIMEOUT_SEC+2));
		// This time it should work
		writeLockPrecursor = writeReadsemaphore.acquireWriteLockPrecursor(key, WRITE_LOCK_PRECURSOR_TIMEOUT_SEC);
		assertNotNull(writeLockPrecursor);
	}
	
	@Test
	public void testForcedReadLockRelease() throws InterruptedException{
		long timeoutOne = 2;
		long timeoutTwo = 4;
		long maxWaitMS = (timeoutOne+timeoutTwo)*2*1000;
		// Get two read locks on that expires after two seconds and another the expires after 4
		String readLockOne = writeReadsemaphore.acquireReadLock(key, timeoutOne);
		assertNotNull(readLockOne);
		String readLockTwo = writeReadsemaphore.acquireReadLock(key, timeoutTwo);
		assertNotNull(readLockTwo);
		// Get the precursor
		String writeLockPrecursor = writeReadsemaphore.acquireWriteLockPrecursor(key, WRITE_LOCK_PRECURSOR_TIMEOUT_SEC);
		assertNotNull(writeLockPrecursor);
		long start = System.currentTimeMillis();
		String writeLock = null;
		do{
			// try to get writeLock
			writeLock = writeReadsemaphore.acquireWriteLock(key, writeLockPrecursor, 1);
			assertTrue("Timed-out waiting for read-locks to expire", (System.currentTimeMillis()-start) < maxWaitMS);
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
		long timeoutOne = 4;
		long maxWaitMS = timeoutOne*2*1000;
		// First acquire a precursor
		String writeLockPrecursor = writeReadsemaphore.acquireWriteLockPrecursor(key, WRITE_LOCK_PRECURSOR_TIMEOUT_SEC);
		assertNotNull(writeLockPrecursor);
		String writeLock = writeReadsemaphore.acquireWriteLock(key, writeLockPrecursor, timeoutOne);
		assertNotNull(writeLock);
		long start = System.currentTimeMillis();
		String readLock = null;
		do{
			// try to get read lock
			readLock = writeReadsemaphore.acquireReadLock(key, timeoutOne);
			assertTrue("Timed-out waiting for write-locks to expire", (System.currentTimeMillis()-start) < maxWaitMS);
			if(readLock == null){
				System.out.println("Waiting for write-locks to expire...");
				Thread.sleep(1000);
			}
		}while(readLock == null);
		// We should now have the write lock
		assertNotNull(readLock);
	}

}
