# database-semaphore
## Introduction
When working with a cluster of web-servers or worker machines it is common to need to limit the number of concurrent processes that are allowed to perform a given task across the cluster.

For example, assume a cluster of web-servers are generating logs that record web-services requests and pushing these logs to an object store such an S3.  To make use of such logs it would be nice if the logs were collated by time stamp into hourly files.  A basic worker setup to process these logs might be a singleton that coordinates the collation work by queuing lists of files to be collated.  Then a fleet of _n_ workers could be used to concurrently dequeue each list and to perform the actual collation. 

In the above example, we need to ensure only a single instance of the queuing worker can be running at any given time across the entire worker cluster.  Similarly, we want to limit the number of concurrent dequeue workers to ten as we are only willing to dedicates a portion of the available network bandwidth to this problem.

This is a classic problem for a [semaphore](http://en.wikipedia.org/wiki/Semaphore_%28programming%29).  While a binary semaphore could be used for the singleton case we would still need a counting semaphore for the multiple instance case.  Since a counting semaphore works for both cases, that is what is provided.

### Example
The following example illustrates how to use the semaphore to gate the run of a worker called 'foo' to ensure there are never more than twenty instance of 'foo' running across the cluster concurrently.  In this example, the following code would be run from a timer that fires every minute:
````java
final String lockKey = "foo";
final long lockTimeoutSec = 10L;
final int maxLockCount = 20;
// attempt to get a lock
final String lockToken = semaphore.attemptToAcquireLock(lockKey, lockTimeoutSec, maxLockCount);
// Only proceed if a lock was acquired
if(lockToken != null){
	try{
		// Run the worker passing a callback to capture progress.
		worker.run(new ProgressCallback() {
			public void progressMade() {
				// Give the lock more time
				semaphore.refreshLockTimeout(lockKey, lockToken, lockTimeoutSec);
					}
				});
	}finally{
		// Unconditionally release the lock.
		semaphore.releaseLock(lockKey, lockToken);
	}
}
````
### Robustness
If the machine that was running the our singleton worker were to suddenly disappeared, we would not want the entire process to come to a halt. Instead, we want one, and only one, new instance to start and take over the singleton's task.  We would want a similar level of robustness for the multiple instance case.

In the above example robustness is achieved by running the code in a timer combined with the use of a lock timeout.  The timer ensure a new worker attempts to acquire the lock every minute while, the lock timeout ensure that if the original lock holder were to fail unexpectedly, their token would expire allowing a new instance to pickup the lock.

### Lock timeouts
Once lock timeout is introduced the next big question is always how much time is enough? If the timeout is too small, there is risk that another worker could start while the original worker is still working.  If the timeout is too large, then the failure recovery time is increased.  A happy middle ground seem to be a small timeout with a mechanism for the worker to refresh the timeout as it makes progress.  In the above example, as long as the worker calls callback.progressMade() from its inner loops, the timeout can be kept small and the worker can run for a very long time.

###  Non-Blocking
It is important point out the semaphore.attemptToAcquireLock() will fail quickly if a lock is unavailable as opposed to blocking the caller until a lock is available.  This allows the caller to quickly free up any resources such a threads or memory when a lock is unavailable.

### Deadlock
A classic deadlock scenario requires at least two thread, at least two locks and blocking or waiting for unavailable locks.  
For example, thread A holds lock 'foo' and thread B holds lock 'bar'.  
Then if thread A attempts to get lock 'bar' and thread B attempts to get lock 'foo' deadlock would be possible if the attempt to get the lock were to be blocking or if the caller waits for the second lock to become available while holding the first.  
Since the semaphore.attemptToAcquireLock() call is non-blocking, deadlock is only possible if each thread were to wait for the second lock while holding the first.
We recommend that caller never waits for locks. However, if you must wait for a lock then it becomes your responsibility to prevent deadlocks using standard techniques.  For example, all processes should acquire locks in the same order.  Or, limit one lock per processes.

### Database Exclusive Locks
To prevent race conditions when more than one instances attempts to acquire the same lock at the same time, an exclusive row level lock is used to ensure all lock request for a given key are process serially (as opposed to concurrently).  This row level lock is only held for a very short window of time (only long enough to check if a lock is available and to issue a lock).

We have learned there are many ways to do this incorrectly which can lead to deadlocks and performance issues.

## Build
This project includes integration tests that must run against a MySQL database.  In order to run these tests
the following system properties must be provide:
* jdbc.url
* jdbc.username
* jdbc.password

### mvn
````
mvn clean install -Djdbc.url=jdbc:mysql://localhost/semaphore -Djdbc.username=<username> -Djdbc.password=<password>
````
### Eclipse
Add the following the "VM Arguments" for the runner:
````
"-Djdbc.url=jdbc:mysql://localhost/semaphore"
"-Djdbc.username=<username>"
"-Djdbc.password=<password>"
````
### Dependencies
This project depends on spring-jdbc 4.1.6.RELEASE or newer.  All Spring dependencies are marked with a scope=provided so it should work with your version of Spring.
The CountingkSemaphoreImpl is a simple Java object so it should be usable with any Inversion Of Control (IoC) system.  This project uses Spring IoC for testing only.  

 
