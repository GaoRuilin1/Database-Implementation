package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import java.util.Stack;
/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement

        // if the current lock type is none of the above cases, return;
//        if (LockType.substitutable(effectiveLockType, requestType)
//                && effectiveLockType.equals(requestType)
//                && (explicitLockType != LockType.IX || requestType != LockType.S)
//                && (!explicitLockType.isIntent())) {
//            return;
//        }

        // if the current lock type can effectively substitute the requested type
        if (LockType.substitutable(effectiveLockType, requestType) || effectiveLockType.equals(requestType)) {
            return;
        }
        // if the current lock type is IX and the requested lock is S
        if (explicitLockType.equals(LockType.IX) && requestType.equals(LockType.S)) {
            callingMyAncestors(transaction, lockContext, LockType.SIX);
            lockContext.promote(transaction, LockType.SIX);
            return;
        }

        //updates the AncestorLocks needed to obtain the requested locktype
        callingMyAncestors(transaction, lockContext, requestType);

        // If the current locktype is an Intent Lock
        if (explicitLockType.isIntent()) {
            lockContext.escalate(transaction);
            // If the explicit locktype is IS then after escalation the locktype should be S
            // So we have to update the locktype to X if X is was requested.
            if (explicitLockType.equals(LockType.IS) && requestType.equals(LockType.X)) {
                lockContext.promote(transaction, requestType);
            }

        }
        // This case is when there is no current lock being held
        else if (explicitLockType.equals(LockType.NL)){
            lockContext.acquire(transaction, requestType);
        }
        // this case is when there exist a lock in the resource and we try to get an X lock
        else {
            lockContext.promote(transaction, requestType);
        }
        return;
    }

    // TODO(proj4_part2) add any helper methods you want

    /**
     * Helper method to call the ancestors of the lockContext
     * We will check if the parent lock matches ensureSufficientLockHeld's
     * cases by checking each parent's lock type from the root to the current
     * lockContext's parent.
     * @param transaction
     * @param lockContext
     * @param requestLockType
     *
     */
    private static void callingMyAncestors(TransactionContext transaction,
                                           LockContext lockContext,
                                           LockType requestLockType) {
        LockContext dadsContext = lockContext.parentContext();
        // if the lockContext is the root, return
        if (dadsContext == null) {
            return;
        }
        // load the ancestors stack with the parents of the lockContext and
        // the locktype needed to obtain the requested locktype
        Stack<LockContext> ancestors = new Stack<>();
        Stack<LockType> ancestorLock = new Stack<>();
        while (dadsContext != null) {
            ancestors.add(dadsContext);
            dadsContext = dadsContext.parentContext();
            ancestorLock.add(LockType.parentLock(requestLockType));
        }

        // Use the ancestors stack to call ensureSufficientLockHeld
        while (!ancestors.empty()) {
            LockContext currentDad = ancestors.pop();
            LockType dadLockType = currentDad.getExplicitLockType(transaction);
            LockType dadRequestLockType = ancestorLock.pop();

            // if the current lock type takes higher priority, continue
            // i.e. they are substitutable if the explicitLockType == locktype
            if (LockType.substitutable(dadLockType, dadRequestLockType)
                    || (dadLockType.equals(LockType.SIX) && dadRequestLockType.equals(LockType.IX))) {
                continue;
            }

            // if the parent has no lock
            if (dadLockType.equals(LockType.NL)) {
                currentDad.acquire(transaction, dadRequestLockType);
            }
            // if the current locktype is an S and we are changing it to an IX
            else if (dadLockType.equals(LockType.S) && dadRequestLockType.equals(LockType.IX)) {
                currentDad.promote(transaction, LockType.SIX);
            }
            // if the current lock is just substitutable
            else {
                currentDad.promote(transaction, dadRequestLockType);
            }
        }

    }
}
