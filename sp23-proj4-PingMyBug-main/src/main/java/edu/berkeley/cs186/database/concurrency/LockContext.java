package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement

        // if context is readonly
        checkReadOnly();

        // if the lock cannot be the parent lock of this lock.
        if (this.parent != null
                && !LockType.canBeParentLock(
                        this.parent.getExplicitLockType(transaction)
                , lockType)){
            throw new InvalidLockException(lockType.name() + " is cannot be "
                    + " the child lock of the parent's ");
        }

        // acquire the lock
        lockman.acquire(transaction, this.getResourceName(), lockType);

        // update numChildLocks by adding 1 to the number of locks on children
        if (this.parent != null) {
            this.parent.numChildLocks.put(transaction.getTransNum() ,
                    this.parent.getNumChildren(transaction) + 1);
        }
        return;
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement

        // if context is readonly
        checkReadOnly();
        // multigranularity locking constraints:
        // if the number of locks on children is larger than 0 because the lock
        // cannot be released
        if (this.getNumChildren(transaction) > 0) {
            throw new InvalidLockException("the lock cannot be released because "
                    + "doing so would violate multigranularity locking constraints"
                    + "meaning the number of locks on children is bigger than 0");
        }

        // release the lock
        this.lockman.release(transaction, this.getResourceName());

        // update numChildLocks by subtracting 1 to the number of locks on children
        if (this.parent != null) {
            this.parent.numChildLocks.put(transaction.getTransNum() ,
                    this.parent.getNumChildren(transaction) - 1);
        }
        return;
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement

        // if context is readonly
        checkReadOnly();

        // if lock type is substitutable for the current lock type and the new
        // lock type is not SIX and the new lock type is not the explicit lock
        LockType explicitLock = this.getExplicitLockType(transaction);
        if (LockType.substitutable(newLockType, lockman.getLockType(transaction, getResourceName()))
                && !newLockType.equals(LockType.SIX) && !newLockType.equals(explicitLock)) {
            // promote the lock
            this.lockman.promote(transaction, this.name, newLockType);
        }
        // if new lock type is SIX and the explicit lock is IS/IX/S
        else if ((newLockType.equals(LockType.SIX)
                && !hasSIXAncestor(transaction)
                && (explicitLock.equals(LockType.IS)
                || explicitLock.equals(LockType.IX)
                || explicitLock.equals(LockType.S)))) {

            List<ResourceName> descendants = this.sisDescendants(transaction);
            List<LockContext> descendantContexts = new ArrayList<>();

            // add the current context to the list of descendants
            descendants.add(this.getResourceName());
            for (ResourceName name : descendants) {
                descendantContexts.add(LockContext.fromResourceName(this.lockman, name));
            }

            // acquire the new lock and release the old lock
            this.lockman.acquireAndRelease(transaction,
                    this.getResourceName(), newLockType, descendants);

            // update numChildLocks by subtracting 1 to the number of locks
            // on each descendant context
            for (LockContext context : descendantContexts) {
                context.numChildLocks.put(transaction.getTransNum(),
                        this.parent.getNumChildren(transaction) - 1);
            }
        } else {
            throw new InvalidLockException(newLockType.name()+ " type is not "
                    + "promotable");
        }

        // Release all S and IS locks on descendants.
        if (newLockType == LockType.SIX) {
            List<ResourceName> descendants = sisDescendants(transaction);
            for (ResourceName descendant : descendants) {
                this.lockman.release(transaction, descendant);
            }
        }
        return;
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement

        // if context is readonly
        checkReadOnly();

        // if the transaction has no lock at this level
        if (this.getExplicitLockType(transaction) == LockType.NL) {
            throw new NoLockHeldException("No lock held by transaction");
        }

        // if the transaction has descendant locks
        List<Lock> descendantLocks = this.lockman.getLocks(transaction);
        List<ResourceName> descendants = new ArrayList<>();

        // add the current context's resource names to the list of descendants
        for (Lock lock : descendantLocks) {
            if (lock.name.isDescendantOf(this.getResourceName())) {
                descendants.add(lock.name);
            }
        }

        // add the current context's resource name to the list of descendants
        List<ResourceName> resToRelease = new ArrayList<>(descendants);
        resToRelease.add(this.getResourceName());

        // If there is an IS or S, we escalate to S
        if (this.getExplicitLockType(transaction).equals(LockType.IS)) {
            this.lockman.acquireAndRelease(transaction, getResourceName(),
                    LockType.S, resToRelease);
            this.numChildLocks.put(transaction.getTransNum(), 0);

        }
        // If there is a SIX or X, we escalate to X
        else if (this.getExplicitLockType(transaction).equals(LockType.SIX)
                || this.getExplicitLockType(transaction).equals(LockType.IX)) {
            this.lockman.acquireAndRelease(transaction, this.getResourceName(),
                    LockType.X, resToRelease);
            this.numChildLocks.put(transaction.getTransNum(), 0);

        }
        return;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        return this.lockman.getLockType(transaction, this.getResourceName());
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement

        // if there is an explicit lock, return it
        LockType explicitLockType = this.getExplicitLockType(transaction);
        if (!explicitLockType.equals(LockType.NL)) {
            if (explicitLockType.equals(LockType.IX) && this.hasSIXAncestor(transaction)) {
                return LockType.SIX;
            }
            return explicitLockType;
        }

        // implicit lock is a lock at an ancestor.
        // so, if there is no explicit lock, check if there is an implicit lock
        LockContext dad = this.parent;
        while (dad != null) {
            LockType implicit = dad.getExplicitLockType(transaction);
            // if the explicit lock is substitutable to the implicit lock
            // and not NL
            if (LockType.substitutable(implicit, LockType.S)) {
                return dad.getExplicitLockType(transaction);
            }
            // if parent is S or X
            if (implicit.equals(LockType.S) || implicit.equals(LockType.X)) {
                return implicit;
            }
            // if parent is SIX return S
            else if (implicit.equals(LockType.SIX)) {
                return LockType.S;
            }
            // if IX, then we need to check if there is SIX ancestor. If there
            // is, return SIX else return IX
            else if (implicit.equals(LockType.IX)) {
                if (dad.hasSIXAncestor(transaction)) {
                    return LockType.SIX;
                }
                return implicit;
            }

            // Check the next ancestor for an implicit lock
            dad = dad.parent;
        }

        return LockType.NL;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        if (this.parent == null) {
            return false;
        } else if (this.getExplicitLockType(transaction) == LockType.SIX
                && this.getResourceName().isDescendantOf(this.getResourceName())) {
            return true;
        } else {
            return this.parent.hasSIXAncestor(transaction);
        }
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        // Start by getting all the locks held by the transaction
        List<Lock> locks = lockman.getLocks(transaction);

        // Create an empty list to store the descendant ResourceNames that have a lock of type IS or S
        List<ResourceName> sisDescendants = new ArrayList<>();

        // Iterate through each lock held by the transaction
        for (Lock lock : locks) {
            // Check if the lock is of type IS or S, and if it has an ancestor that is the current LockContext
            if ((lock.lockType == LockType.IS || lock.lockType == LockType.S) && lock.name.isDescendantOf(name)) {
                // If it does, add the ResourceName of the lock to the list of descendant ResourceNames
                sisDescendants.add(lock.name);
            }
        }

        // Return the list of descendant ResourceNames
        return sisDescendants;
    }
    /**
     * @throws UnsupportedOperationException if context is readonly
     */
    protected void checkReadOnly() {
        if (this.readonly) {
            throw new UnsupportedOperationException("cannot modify readonly " +
                    "context");
        }
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

