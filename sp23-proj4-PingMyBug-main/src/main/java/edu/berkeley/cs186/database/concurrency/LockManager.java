package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
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
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
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
            for (Lock lock : locks) {
                // if a transaction isn't trying to replace a lock it already
                // has on the resource
                if (lock.transactionNum != except) {
                    // if current lock is not compatible with the new lock
                    return LockType.compatible(lock.lockType, lockType);
                }
            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement
            for (Lock l : locks) {
                // if the transaction already has a lock
                if (l.transactionNum == lock.transactionNum) {
                    // update the lock
                    l.lockType = lock.lockType;
                    return;
                }
            }
            // if the transaction doesn't already have a lock
            if (transactionLocks.containsKey(lock.transactionNum)) {
                // add the lock to the transaction's list of locks
                transactionLocks.get(lock.transactionNum).add(lock);
            } else { // if the transaction doesn't have any locks
                List<Lock> newLocks = new ArrayList<>();
                // add the lock to the transaction's list of locks
                newLocks.add(lock);
                transactionLocks.put(lock.transactionNum, newLocks);
            }

            locks.add(lock); // add the lock to the resource's list of locks
            return;
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement

            // remove the lock from the transaction's list of locks
            transactionLocks.get(lock.transactionNum).remove(lock);
            // remove the lock from the resource's list of locks
            locks.remove(lock);
            // process the queue
            processQueue();
            return;
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement

            if (addFront) {
                waitingQueue.addFirst(request);
            } else {
                waitingQueue.addLast(request);
            }
            return;
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();

            // TODO(proj4_part1): implement

            // The main idea is to iterate through the queue and grant locks
            // to requests from front to back of the queue, stopping when the
            // next lock cannot be granted. Once a request is completely
            // granted, the transaction that made the request can be unblocked.
            while (requests.hasNext()) {
                LockRequest req = requests.next();
                // if the lock can be granted to the transaction in the request
                if (checkCompatible(req.lock.lockType, req.lock.transactionNum)) {
                    // grant the lock to the transaction in the request
                    grantOrUpdateLock(req.lock);
                    requests.remove(); // remove the request from the queue
                    // release the locks that were released in the request
                    for (Lock lockToRelease : req.releasedLocks) {
                        // if the lock to release is still in the resource's list of locks
                        if (getResourceEntry(lockToRelease.name).locks.contains(lockToRelease)) {
                            // release the lock
                            getResourceEntry(lockToRelease.name).releaseLock(lockToRelease);
                        }
                    }
                    // Unblock the transaction in the request since the
                    // request was granted
                    req.transaction.unblock();
                } else { // stop when the next lock in the queue cannot be granted
                    break;
                }
            }
            return;
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            for (Lock l : locks) {
                // if the transaction already has a lock
                if (l.transactionNum == transaction) {
                    // return the lock type of the transaction.
                    return l.lockType;
                }
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
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
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

            ResourceEntry resEntered = getResourceEntry(name);

            // error check first:
            // if the transaction has the lock on name isn't releasing it
            hasLockOnNameButNotReleasing(name, transaction, releaseNames);

            // if the transaction doesn't hold a lock on one or more of the
            // names in releaseNames
            noLockOnNameOneOrMore(transaction, releaseNames);

            Lock lockOnName = new Lock(name, lockType, transaction.getTransNum());

            // if the new lock is not compatible with another transaction's lock
            // on the resource
            if (!resEntered.checkCompatible(lockType, transaction.getTransNum())) {
                // block the transaction
                transaction.prepareBlock();
                shouldBlock = true;

                // add the request to the front of the queue
                LockRequest req = new LockRequest(transaction, lockOnName);
                resEntered.addToQueue(req, true);
            } else {
                // release all locks on releaseNames held by the transaction
                for (ResourceName releaseName : releaseNames) {
                    for (Lock l : getLocks(transaction)) {
                        if (l.name.equals(releaseName)) {
                            // release the lock
                            getResourceEntry(releaseName).releaseLock(l);
                            // process the queue
                            resEntered.processQueue();
                        }
                    }
                }

                // acquire the lock on name
                resEntered.grantOrUpdateLock(lockOnName);
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            ResourceEntry resEntered = getResourceEntry(name);
            Long transNum = transaction.getTransNum();
            Boolean compatible = resEntered.checkCompatible(lockType, transNum);

            // error check first:
            // if the transaction already has a lock on the resource
            hasLockOnName(transaction, name);

            // Check for compatibility with other locks:
            // if the lock is compatible and there are no other transactions
            // waiting for the lock
            if (compatible && resEntered.waitingQueue.isEmpty()) {
                // grant the lock to the transaction
                resEntered.grantOrUpdateLock(new Lock(name, lockType, transNum));
            } else {
                // if the lock is not compatible or there are other transactions
                // waiting for the lock:

                // add to the back of the queue
                LockRequest req = new LockRequest(transaction,
                        new Lock(name, lockType, transNum));
                resEntered.addToQueue(req, false);

                // prepare to block
                transaction.prepareBlock();
                shouldBlock = true;
            }
        }
        // if the transaction should be blocked, block it
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
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

            ResourceEntry resEntered = getResourceEntry(name);

            // error check first:
            // if the transaction does not have a lock on the resource
            noLockOnName(name);

            // if the entered resource has a lock on it:
            // release the lock
            LockType l = resEntered.getTransactionLockType(transaction.getTransNum());
            resEntered.releaseLock(new Lock(name, l,
                    transaction.getTransNum()));

            // process the queue
            resEntered.processQueue();
        }

    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        boolean shouldBlock = false;
        synchronized (this) {

            ResourceEntry resEntered = getResourceEntry(name);

            // error checks first:
            // if a transaction already has a newLockType lock on name
            hasSameLockOnName(transaction, name, newLockType);
            // if the transaction does not have a lock on the resource
            noLockOnName(name);
            // if lock type B is not substitutable for A
            LockType oldLockType = getLockType(transaction, name);
            if (!LockType.substitutable(newLockType, oldLockType)
                    || newLockType.equals(oldLockType)) {
                throw new InvalidLockException("Lock type " + newLockType.name()
                        + " is not " + "substitutable for "
                        + oldLockType.name() + " or " + newLockType.name()
                        + " is equal to " + oldLockType.name());
            }
            // if the lock is a SIX, no promotion is allowed
            invalidLockType(newLockType, LockType.SIX);

            // if the lock is a valid promotion
            if (resEntered.checkCompatible(newLockType, transaction.getTransNum())) {
                // promote the lock
                resEntered.grantOrUpdateLock(new Lock(name, newLockType,
                        transaction.getTransNum()));
            } else {
                // add to the front of the queue
                LockRequest req = new LockRequest(transaction,
                        new Lock(name, newLockType,
                                transaction.getTransNum()));
                resEntered.addToQueue(req, true);

                // prepare to block
                transaction.prepareBlock();
                shouldBlock = true;
            }

        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return DuplicateLockRequestException the transaction already has a
     * lock on the resource
     * */
    protected void hasLockOnName(TransactionContext transaction,
                               ResourceName name) {
        for (Lock l : getLocks(name)) {
            if (l.transactionNum == transaction.getTransNum()) {
                throw new DuplicateLockRequestException("This resource is held"
                        + " by transaction: " + transaction.getTransNum());
            }
        }
    }

    /**
     * Return DuplicateLockRequestException if the transaction already has a
     * lock on the resource and isn't releasing the lock on name
     * */
    protected void hasLockOnNameButNotReleasing(ResourceName name,
                               TransactionContext transaction,
                                              List<ResourceName> releaseName) {
        for (Lock l : getLocks(name)) {
            if (l.transactionNum == transaction.getTransNum() &&
                    !releaseName.contains(l.name)) {
                throw new DuplicateLockRequestException("This resource is held"
                        + " by transaction: " + transaction.getTransNum()
                        + " and is not releasing it");
            }
        }
    }

    /**
     * Return DuplicateLockRequestException if the transaction already
     * has a newLockType lock on name
     */
    protected void hasSameLockOnName(TransactionContext transaction,
                               ResourceName name,
                               LockType newLockType) {
        for (Lock l : getLocks(name)) {
            if (l.transactionNum == transaction.getTransNum()
                    && l.lockType.equals(newLockType)) {
                throw new DuplicateLockRequestException("This resource is held"
                        + " by transaction: " + transaction.getTransNum()
                        + " and has the same lock type as the new lock type: "
                        + newLockType.name());
            }
        }
    }

    /**
     * Return NoLockHeldException if the transaction does not have a lock
    * */
    protected void noLockOnName(ResourceName name) {
        if (getLocks(name).size() == 0) {
            throw new NoLockHeldException("This transaction does not have " +
                    "a lock on this resource name.");
        }
    }

    /**
     * Return NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     * */
    protected void noLockOnNameOneOrMore(TransactionContext transaction,
                              List<ResourceName> releaseNames) {
        int count = 0;
        for (ResourceName name : releaseNames) {
            for (Lock l : getLocks(transaction)) {
                if (l.name == name) {
                    count++;
                }
            }
        }
        if (count != releaseNames.size()) {
            throw new NoLockHeldException("This transaction does not hold " +
                    "a lock on one or more of the names in releaseNames");
        }
    }

    /**
     * Return InvalidLockException current lock type cannot be some specified
     * restricted lock type.
     * */
    protected void invalidLockType(LockType currLockType,
                                   LockType restrictedLockType) {
        if (currLockType.equals(restrictedLockType)) {
            throw new InvalidLockException("We do not allow promotions " +
                    "to " + restrictedLockType.name());
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        for (Lock l : getLocks(name)) {
            // If the lock is held by the transaction, return the lock type
            if (l.transactionNum.equals(transaction.getTransNum())) {
                return l.lockType;
            }
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
