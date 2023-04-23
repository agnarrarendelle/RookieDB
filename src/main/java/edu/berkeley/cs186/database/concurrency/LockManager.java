package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 * <p>
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 * <p>
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 * <p>
 * This does mean that in the case of:
 * queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement
            //Iterate through all locks on a resource
            for (Lock lock : this.locks) {
                //If the current lock is from transaction K, where K equals to {except}
                //(The transaction might want to upgrade its lock on the resource) or
                //If the new lock is compatible with all existing locks
                //return True
                //Else, return False
                if (lock.transactionNum != except && !LockType.compatible(lock.lockType, lockType)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock newLock) {
            // TODO(proj4_part1): implement
            //Get the transaction id of the new lock
            long currTransactionNum = newLock.transactionNum;

            //If the lock is the first lock that a transaction requests,
            //update {transactionLocks} to store the info of the new transaction that sends the lock request
            if (!transactionLocks.containsKey(currTransactionNum)) {
                transactionLocks.put(currTransactionNum, new ArrayList<>());
            }

            //Get the list of existing locks that the transaction the new lock belongs to has on this resource
            List<Lock> currTransactionLocks = transactionLocks.get(currTransactionNum);

            //Iterate through all existing locks
            for (Lock lock : currTransactionLocks) {
                //If the lock want to lock the same resource of the current lock and
                //the new lock is not the same type of locks as the current lock
                //simply update the current lock's type to that of the new lock and return
                //(Lock Update)
                boolean isOnSameResource = lock.name.equals(newLock.name);
                boolean isSameTypeOfLock = lock.lockType == newLock.lockType;
                if (isOnSameResource && !isSameTypeOfLock) {
                    lock.lockType = newLock.lockType;
                    return;
                }
            }

            //If the new lock cannot be updated from the existing locks,
            //simply add the new lock to the transaction's existing locks
            //as well as the resource's existing lock list and return
            //(Lock Grant)
            currTransactionLocks.add(newLock);
            locks.add(newLock);
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement
            //get the id of the transaction that the lock comes from
            long currTransactionNum = lock.transactionNum;
            //remove the lock from the transaction's existing lock list
            //as well as the resource's existing lock list
            transactionLocks.get(currTransactionNum).remove(lock);
            locks.remove(lock);
            //process the lock request queue
            processQueue();
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement
            if (addFront)
                waitingQueue.addFirst(request);
            else
                waitingQueue.addLast(request);
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();

            //Loop until there is not more lock request on the resource
            while (requests.hasNext()) {
                //get the current lock request
                LockRequest currLockRequest = requests.next();
                Lock requestLock = currLockRequest.lock;
                //get the transaction id that sends the current lock request
                long transNumOfCurrLockRequest = currLockRequest.transaction.getTransNum();

                //check if the current lock request is compatible with the existing locks on this resource
                boolean isLockCompatible = checkCompatible(requestLock.lockType, transNumOfCurrLockRequest);

                //If it is a conflicting request, simpy return and stop processing the queue
                if (!isLockCompatible) return;

                //remove the current request from the wait queue as it will be processed later
                this.waitingQueue.removeFirst();

                //grant the current lock request access to the resource or
                //updating an existing lock to the current lock's type
                grantOrUpdateLock(requestLock);

                //release all locks that the transaction the sends the current request that need to be released
                for (Lock releasedLock : currLockRequest.releasedLocks) {
                    release(currLockRequest.transaction, releasedLock.name);
                }

                //unblock the transaction as the request has been granted
                currLockRequest.transaction.unblock();
            }

        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            for (Lock lock : locks) {
                if (lock.transactionNum == transaction)
                    return lock.lockType;
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     * <p>
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     * <p>
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     * <p>
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     *                                       by `transaction` and isn't being released
     * @throws NoLockHeldException           if `transaction` doesn't hold a lock on one
     *                                       or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            //get the resource object itself with the name
            ResourceEntry resourceEntry = getResourceEntry(name);
            long transNum = transaction.getTransNum();

            //If the transaction already has a lock of the same type as the new lock on the resource
            //and the existing lock will not be released after the new lock has been added to the resource,
            //throw an exception and return
            boolean hasSameLockType = resourceEntry.getTransactionLockType(transNum) == lockType;
            boolean willOldLockBeReleased = releaseNames.contains(name);
            if (hasSameLockType && !willOldLockBeReleased)
                throw new DuplicateLockRequestException(String.format("Transaction %s already has a lock of type %s on resource %s", transaction, lockType, resourceEntry));


            //create the new lock
            Lock newLock = new Lock(name, lockType, transNum);

            //If the new lock is compatible with locks in the grant set of the resource,
            //release all locks the transaction has in {releaseNames}
            //except for the lock that is on the same resource as the new lock.
            //After releasing all locks that needs to be released,
            //add the new lock to the grant set of the resource and return
            if (resourceEntry.checkCompatible(lockType, transNum)) {
                for (ResourceName releaseName : releaseNames) {
                    if (releaseName.equals(name))
                        continue;
                    release(transaction, releaseName);
                }
                resourceEntry.grantOrUpdateLock(newLock);
                return;
            }

            //If the new lock is NOT compatible with locks in the grant set of the resource,
            //create a list of locks the transaction has on each resource in {releaseNames}
            //iterate through all resource that needs to be freed and get the lock the transaction has on them.
            //add the lock to the list.
            //If the transaction has no lock on the current resource, throw an exception
            List<Lock> releaseLocks = new ArrayList<>();
            for (ResourceName releaseName : releaseNames) {
                LockType releasedLockType = getLockType(transaction, releaseName);
                if (releasedLockType == LockType.NL)
                    throw new NoLockHeldException(String.format("Transaction %s does not hold a lock on resource %s", transaction, releaseName));

                Lock lockToRelease = new Lock(releaseName, releasedLockType, transNum);
                releaseLocks.add(lockToRelease);
            }

            //add the new lock request to the FRONT of the waiting queue of the resource
            //with all the locks that need to be released
            //and prepare to block the transaction
            resourceEntry.addToQueue(new LockRequest(transaction, newLock, releaseLocks), true);
            shouldBlock = true;
            transaction.prepareBlock();
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     * <p>
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     *                                       `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            //get the resource object itself with the name
            ResourceEntry resourceEntry = getResourceEntry(name);

            //get the lock type of the existing lock on the resource from the transaction
            LockType transactionLockTypeOnResourceEntry = resourceEntry.getTransactionLockType(transaction.getTransNum());
            //If the transaction already has a lock of the same type as the new lock request on the resource
            //throw an exception and return
            if (transactionLockTypeOnResourceEntry == lockType)
                throw new DuplicateLockRequestException(String.format("Transaction %s already has lock type %s on resource %s", transaction.getTransNum(), transactionLockTypeOnResourceEntry, resourceEntry));

            //create the new lock object
            Lock newLockFromTransaction = new Lock(name, lockType, transaction.getTransNum());

            //check if the new lock can be added directly into the existing grant set of the resource
            //or needs to be put at the end of the waiting queue of the resource
            boolean needsToBlock = !resourceEntry.checkCompatible(lockType, transaction.getTransNum()) || resourceEntry.waitingQueue.size() > 0;

            //If the new lock is compatible with all the existing locks in the grant set of the source and
            //there is no other request in the waiting queue of the resource
            //simply grant(or update) the lock to the resource and return
            if (!needsToBlock) {
                resourceEntry.grantOrUpdateLock(newLockFromTransaction);
                return;
            }


            //add the new lock to the end of the waiting queue of the resource
            //and prepare to block the transaction
            resourceEntry.addToQueue(new LockRequest(transaction, newLockFromTransaction), false);
            shouldBlock = true;
            transaction.prepareBlock();

        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     * <p>
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {
            //get the transaction's lock type on the resource
            LockType requestLockType = getLockType(transaction, name);

            //If the lock type is NL, it means the transaction has no lock on the resource,
            //so throw an exception and return
            if (requestLockType == LockType.NL)
                throw new NoLockHeldException(String.format("Transaction %s has no lock on %s", transaction.getTransNum(), name.toString()));

            //get the resource object itself with the same
            ResourceEntry resource = getResourceEntry(name);

            //make the resource release the lock that the transaction holds
            resource.releaseLock(new Lock(name, requestLockType, transaction.getTransNum()));
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     * <p>
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     * <p>
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     *                                       `newLockType` lock on `name`
     * @throws NoLockHeldException           if `transaction` has no lock on `name`
     * @throws InvalidLockException          if the requested lock type is not a
     *                                       promotion. A promotion from lock type A to lock type B is valid if and
     *                                       only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        boolean shouldBlock = false;
        synchronized (this) {
            //get the resource object with the name
            ResourceEntry resourceEntry = getResourceEntry(name);
            //get the type of the existing lock on the resource from the transaction
            LockType oldLockType = getLockType(transaction, name);

            //Throw exception if:
            //1. The new lock has the same type of the old lock
            //2. The transaction has no previous lock on the resource
            //3. The new old lock cannot be updated to new lock(the types are conflicting)
            if (oldLockType == newLockType) {
                throw new DuplicateLockRequestException(String.format("Transaction %s already has a %s lock on resource %s", transaction, newLockType, resourceEntry));
            }
            if (oldLockType == LockType.NL) {
                throw new NoLockHeldException(String.format("Transaction %s has no lock on resource %s", transaction, resourceEntry));
            }
            if (!LockType.substitutable(newLockType, oldLockType)) {
                throw new InvalidLockException(String.format("Lock type %s is cannot substitute for lock type %s", newLockType, oldLockType));
            }

            //create the new lock
            Lock newLock = new Lock(name, newLockType, transaction.getTransNum());

            //If the new lock is compatible with the locks in the grant set of the resource,
            //simple add it to the grant set(or update) and return
            if (resourceEntry.checkCompatible(newLockType, transaction.getTransNum())) {
                resourceEntry.grantOrUpdateLock(newLock);
                return;
            }

            //If the new lock cannot be added to the grant set directly,
            //add it to the FRONT of the waiting queue of the resource
            //and prepare to block the transaction
            LockRequest newLockRequest = new LockRequest(transaction, newLock);
            resourceEntry.addToQueue(newLockRequest, true);
            shouldBlock = true;
            transaction.prepareBlock();
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        ResourceEntry resourceEntry = getResourceEntry(name);
        for (Lock lock : resourceEntry.locks) {
            if (lock.transactionNum == transaction.getTransNum())
                return lock.lockType;
        }
        return LockType.NL;
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}
