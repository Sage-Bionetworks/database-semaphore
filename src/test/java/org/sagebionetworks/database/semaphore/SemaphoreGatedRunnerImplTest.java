package org.sagebionetworks.database.semaphore;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class SemaphoreGatedRunnerImplTest {

	CountingSemaphore mockSemaphore;
	SemaphoreGatedRunnerImpl gate;
	ProgressingRunner mockRunner;
	String lockKey;
	long lockTimeoutSec;
	int maxLockCount;
	
	@Before
	public void before(){
		mockSemaphore = Mockito.mock(CountingSemaphore.class);
		mockRunner = Mockito.mock(ProgressingRunner.class);
		lockKey = "aKey";
		lockTimeoutSec = 10;
		maxLockCount = 2;
		gate = new SemaphoreGatedRunnerImpl(mockSemaphore);
		gate.configure(mockRunner, lockKey, lockTimeoutSec, maxLockCount);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testConfigureBad(){
		mockRunner = null;
		gate.configure(mockRunner, lockKey, lockTimeoutSec, maxLockCount);
	}
	
	@Test
	public void testHappy() throws Exception{
		String atoken = "atoken";
		when(mockSemaphore.attemptToAcquireLock(lockKey, lockTimeoutSec, maxLockCount)).thenReturn(atoken);
		// start the gate
		gate.run();
		// runner should be run
		verify(mockRunner).run(any(ProgressCallback.class));
		// The lock should get released.
		verify(mockSemaphore).releaseLock(lockKey, atoken);
		// The lock should not be refreshed for this case.
		verify(mockSemaphore, never()).refreshLockTimeout(anyString(), anyString(), anyLong());
	}
	
	@Test
	public void testLockReleaseOnException() throws Exception{
		// The lock must be released on exception.
		// Simulate an exception thrown by the runner.
		doThrow(new RuntimeException("Something went wrong!")).when(mockRunner).run(any(ProgressCallback.class));
		// Issue a lock.
		String atoken = "atoken";
		when(mockSemaphore.attemptToAcquireLock(lockKey, lockTimeoutSec, maxLockCount)).thenReturn(atoken);
		gate.run();
		// The lock should get released.
		verify(mockSemaphore).releaseLock(lockKey, atoken);
	}
	
	@Test
	public void testLockNotAcquired() throws Exception{
		// Null is returned when a lock cannot be acquired.
		when(mockSemaphore.attemptToAcquireLock(lockKey, lockTimeoutSec, maxLockCount)).thenReturn(null);
		// Start the run
		gate.run();
		// the lock should not be released or refreshed.
		verify(mockSemaphore, never()).refreshLockTimeout(anyString(), anyString(), anyLong());
		verify(mockSemaphore, never()).releaseLock(anyString(), anyString());
		// The worker should not get called.
		verify(mockRunner, never()).run(any(ProgressCallback.class));
	}
	
	@Test
	public void testExceptionOnAcquireLock() throws Exception{
		when(mockSemaphore.attemptToAcquireLock(lockKey, lockTimeoutSec, maxLockCount)).thenThrow(new OutOfMemoryError("Something bad!"));
		// Start the run. The exception should not make it out of the runner.
		gate.run();
		// the lock should not be released or refreshed.
		verify(mockSemaphore, never()).refreshLockTimeout(anyString(), anyString(), anyLong());
		verify(mockSemaphore, never()).releaseLock(anyString(), anyString());
		// The worker should not get called.
		verify(mockRunner, never()).run(any(ProgressCallback.class));
	}
	
	@Test
	public void testProgress() throws Exception{
		String atoken = "atoken";
		when(mockSemaphore.attemptToAcquireLock(lockKey, lockTimeoutSec, maxLockCount)).thenReturn(atoken);
		
		// Setup the runner to make progress at twice
		doAnswer(new Answer<Void>() {

			public Void answer(InvocationOnMock invocation) throws Throwable {
				ProgressCallback callback = (ProgressCallback) invocation.getArguments()[0];
				// once
				callback.progressMade();
				// twice
				callback.progressMade();
				return null;
			}
		}).when(mockRunner).run(any(ProgressCallback.class));
		// start the gate
		gate.run();
		// The lock should get refreshed twice.
		verify(mockSemaphore, times(2)).refreshLockTimeout(lockKey, atoken, lockTimeoutSec);
		// The lock should get released.
		verify(mockSemaphore).releaseLock(lockKey, atoken);
	}
}
