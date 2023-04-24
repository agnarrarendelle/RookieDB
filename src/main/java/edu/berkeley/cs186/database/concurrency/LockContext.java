package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;
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
    protected final LockManager lockMannager;

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
        this.lockMannager = lockman;
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
     * <p>
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException          if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     *                                       transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement

        //read-only node cannot be modified
        if (this.readonly)
            throw new UnsupportedOperationException(String.format("%s is read only", this.name));
        //The transaction cannot acquire same type of lock on a single resource node twice
        if (getExplicitLockType(transaction) == lockType)
            throw new DuplicateLockRequestException(String.format("%s already has a lock at %s", transaction, this.name));

        //A new lock can be acquired on a resource node only
        //if its parent's lock type is compatible with the new lock type
        if (this.parent != null) {
            LockType parentLockType = parent.getExplicitLockType(transaction);
            if (!LockType.canBeParentLock(parentLockType, lockType))
                throw new InvalidLockException(String.format("Lock %s cannot be parent of lock %s", parentLockType, lockType));
        }

        //Add the new lock to the resource node
        lockMannager.acquire(transaction, this.name, lockType);
        //Add one child to this resource node's parent's child number
        updateParentChildNum(transaction, 1, this.parent);
    }

    /**
     * Release `transaction`'s lock on `name`.
     * <p>
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException           if no lock on `name` is held by `transaction`
     * @throws InvalidLockException          if the lock cannot be released because
     *                                       doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement

        //read-only node cannot be modified
        if (this.readonly)
            throw new UnsupportedOperationException(String.format("%s is read only", this.name));

        //Current level node does not hold any lock
        if (getExplicitLockType(transaction) == LockType.NL)
            throw new NoLockHeldException(String.format("Transaction %s does not have any lock on %s", transaction, this.name));

        //Must release locks from bottom-up.
        //A node cannot release its lock until all its children have released theirs
        if (getNumChildren(transaction) > 0)
            throw new InvalidLockException(String.format("Cannot release from %s because the transaction still holds some lock on its children", this.name));

        //Release the lock on this resource node from the transaction
        this.lockMannager.release(transaction, this.name);

        //Remove one child from the parent of this resource node
        //after the lock has been released
        updateParentChildNum(transaction, -1, this.parent);
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     * <p>
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     *                                       `newLockType` lock
     * @throws NoLockHeldException           if `transaction` has no lock
     * @throws InvalidLockException          if the requested lock type is not a
     *                                       promotion or promoting would cause the lock manager to enter an invalid
     *                                       state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     *                                       type B is valid if B is substitutable for A and B is not equal to A, or
     *                                       if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     *                                       be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement

        //read-only node cannot be modified
        if (this.readonly)
            throw new UnsupportedOperationException(String.format("%s is read only", this.name));
        //What lock is on this resource node from the transaction
        LockType oldLockType = getExplicitLockType(transaction);

        //The transaction does not hold any lock on this resource node
        if (oldLockType == LockType.NL)
            throw new NoLockHeldException(String.format("Transaction %s does not have any lock on %s", transaction, this.name));
        //The transaction already has a lock of the same type as the new lock on this resource node
        if (oldLockType == newLockType)
            throw new DuplicateLockRequestException(String.format("Transaction %s already has lock %s on %s", transaction, oldLockType, this.name));
        //The resource node's parent node's lock is not compatible with the new lock
        if (this.parent != null && !LockType.canBeParentLock(this.parent.getExplicitLockType(transaction), newLockType))
            throw new InvalidLockException(String.format("Parent lock %s cannot grand %s lock on child", this.parent.getExplicitLockType(transaction), newLockType));
        //The old lock cannot be upgraded to the new lock
        if (!LockType.substitutable(newLockType, oldLockType))
            throw new InvalidLockException(String.format("Lock %s cannot be replaced by lock %s", oldLockType, newLockType));
        //A lock on a resource node cannot be upgraded to SIX type if
        //one of its ancestor nodes already holds a SIX lock
        if (newLockType == LockType.SIX && hasSIXAncestor(transaction))
            throw new InvalidLockException("Cannot promote a lock with a SIX ancestor to SIX as well");

        //If the new lock is a not SIX lock,
        //simply upgrade the old lock to the new lock
        if (newLockType != LockType.SIX) {
            this.lockMannager.promote(transaction, this.name, newLockType);
            return;
        }

        //If the new lock is a SIX lock, then all S/IS locks on the child nodes of this resource node
        //must be released once the old lock is upgraded to SIX lock

        //Get child nodes that have S/IS locks on them whose locks will be released latter
        List<ResourceName> childNodeWithSorISLock = sisDescendants(transaction);

        //Must add the current level node to the list of nodes to release
        childNodeWithSorISLock.add(this.name);

        //Upgrade the old lock to SIX lock and release all locks on its children that have S/IS locks on them
        this.lockMannager.acquireAndRelease(transaction, this.name, newLockType, childNodeWithSorISLock);

        //Iterate through all child nodes whose locks have been released
        for (ResourceName resourceName : childNodeWithSorISLock) {

            //If the child node's parent(also a child of this resource node)
            LockContext subParent = fromResourceName(this.lockMannager, resourceName).parent;
            assert(subParent != null);
            //removing one child from those parents
            subParent.updateParentChildNum(transaction, -1, subParent);
        }
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     * <p>
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
     * <p>
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException           if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement

        //read-only node cannot be modified
        if (this.readonly)
            throw new UnsupportedOperationException(String.format("%s is read only", this.name));

        LockType oldLockType = getExplicitLockType(transaction);

        //The transaction does not hold any lock on this resource node
        if (oldLockType == LockType.NL)
            throw new NoLockHeldException(String.format("Transaction %s does not have any lock on %s", transaction, this.name));

        //If the resource node holds an S/X lock, then there is no need to escalate
        if (oldLockType == LockType.S || oldLockType == LockType.X)
            return;

        //Get all child nodes that belongs to this resource node
        List<ResourceName> childNodes = getChildNodes(transaction);

        //A list to store all locks that must be released after the escalation
        List<ResourceName> releaseResources = new ArrayList<>();

        //Iterate all child ndoes
        for (ResourceName childNode : childNodes) {
            LockType childNodeLockType = this.lockMannager.getLockType(transaction, childNode);

            //If the child node is not locked by this transaction, skip it
            if (childNodeLockType == LockType.NL)
                continue;

            //mark the lock on the child node in this transaction to be released latter
            releaseResources.add(childNode);

            //removing all grand-child locks number from this child since they will be released as well
            LockContext childNodeLockContext = fromResourceName(this.lockMannager, childNode);
            childNodeLockContext.numChildLocks.put(transaction.getTransNum(), 0);
        }

        //removing all child locks in this resource node as the escalation will release them
        this.numChildLocks.put(transaction.getTransNum(), 0);

        //If the current node has an S/IS lock, the escalation will upgrade it to S lock
        //Else, it will be upgraded to X lock
        LockType currLockType = getExplicitLockType(transaction);
        LockType escalateLockType = (currLockType == LockType.S || currLockType == LockType.IS)
                ? LockType.S
                : LockType.X;

        //Call lock manager to upgrade the lock and release all the child locks
        this.lockMannager.acquireAndRelease(transaction, this.name, escalateLockType, releaseResources);
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        //Call lock manager to get the lock on the Resource Node
        //identified by transaction number and the resource name
        return this.lockMannager.getLockType(transaction, this.name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {

        //Get the lock on the current level resource in the resource tree
        LockType currLevelLockType = getExplicitLockType(transaction);
        //Get the parent's lock above
        LockType parentLockType = (parent == null ? LockType.NL : parent.getEffectiveLockType(transaction));

        //If either the current level node or the parent node holds an X lock
        //return X lock type
        if (currLevelLockType == LockType.X || parentLockType == LockType.X) {
            return LockType.X;
        }
        //If either the current level node or the parent node holds an S/SIX lock
        //return S lock type(SIX lock type grant ALL sub-nodes S lock implicitly, but only ONE X lock type)
        if (currLevelLockType == LockType.SIX || parentLockType == LockType.SIX ||
                currLevelLockType == LockType.S || parentLockType == LockType.S) {
            return LockType.S;
        }
        return LockType.NL;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     *
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        //recursively calls current node's parent until null or a parent that has SIX lock
        if (this.parent == null)
            return false;
        if (this.parent.getExplicitLockType(transaction) == LockType.SIX)
            return true;
        return this.parent.hasSIXAncestor(transaction);
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     *
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        //Get a list of locks that the transaction holds on different resources
        List<Lock> transactionLocks = this.lockMannager.getLocks(transaction);
        //result list
        List<ResourceName> targetResource = new ArrayList<>();

        //Iterate through all locks in this transaction
        //If the lock is on a node that is a child of the current node, and
        //the lock is S/SIX type, add it to the result list
        for (Lock lock : transactionLocks) {
            boolean isLockOnChild = lock.name.isDescendantOf(this.name);
            boolean isLockSIXorS = (lock.lockType == LockType.S || lock.lockType == LockType.SIX);
            if (isLockOnChild && isLockSIXorS)
                targetResource.add(lock.name);
        }

        //return the result list
        return targetResource;
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
        LockContext temp = new LockContext(lockMannager, this, name,
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

    private void updateParentChildNum(TransactionContext transaction, int amount, LockContext parent) {
        //If the parent is not null(not a top-level node)
        //update the parent's child number to (original child num + amount)
        if (parent != null) {
            int oldChildNumOfParent = parent.getNumChildren(transaction);
            parent.numChildLocks.put(transaction.getTransNum(), oldChildNumOfParent + amount);
        }
    }

    //return a list of nodes that are who has this resource node as their parent
    private List<ResourceName> getChildNodes(TransactionContext transaction) {
        //Get a list of lock that the transaction holds
        List<Lock> childNodeLocks = this.lockMannager.getLocks(transaction);

        //result list
        List<ResourceName> childNodes = new ArrayList<>();

        //Iterate all lock in this transaction
        //If the lock is on a node that has this resource node as its parent
        //add it to the result list
        for (Lock lock : childNodeLocks) {
            if (lock.name.isDescendantOf(this.name))
                childNodes.add(lock.name);
        }

        //return the result
        return childNodes;
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

