package nachos.threads;

import nachos.machine.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

/**
 * A scheduler that chooses threads using a lottery.
 *
 * <p>
 * A lottery scheduler associates a number of tickets with each thread. When a
 * thread needs to be dequeued, a random lottery is held, among all the tickets
 * of all the threads waiting to be dequeued. The thread that holds the winning
 * ticket is chosen.
 *
 * <p>
 * Note that a lottery scheduler must be able to handle a lot of tickets
 * (sometimes billions), so it is not acceptable to maintain state for every
 * ticket.
 *
 * <p>
 * A lottery scheduler must partially solve the priority inversion problem; in
 * particular, tickets must be transferred through locks, and through joins.
 * Unlike a priority scheduler, these tickets add (as opposed to just taking
 * the maximum).
 */
public class LotteryScheduler extends PriorityScheduler {
    /**
     * Allocate a new lottery scheduler.
     */
    public LotteryScheduler() {
    }
    
    /**
     * Allocate a new lottery thread queue.
     *
     * @param   transferPriority    <tt>true</tt> if this queue should
     *                  transfer tickets from waiting threads
     *                  to the owning thread.
     * @return  a new lottery thread queue.
     */
    public ThreadQueue newThreadQueue(boolean transferPriority) {
        return new LotteryQueue(transferPriority);
    }
    public int getPriority(KThread thread) {
    Lib.assertTrue(Machine.interrupt().disabled());
               
    return getLotteryThreadState(thread).getPriority();
    }

    public int getEffectivePriority(KThread thread) {
    Lib.assertTrue(Machine.interrupt().disabled());
               
    return getLotteryThreadState(thread).getEffectivePriority();
    }

    public void setPriority(KThread thread, int priority) {
    Lib.assertTrue(Machine.interrupt().disabled());
               
    Lib.assertTrue(priority >= priorityMinimum &&
           priority <= priorityMaximum);
    
    getLotteryThreadState(thread).setPriority(priority);
    }

    public boolean increasePriority() {
    boolean intStatus = Machine.interrupt().disable();
               
    KThread thread = KThread.currentThread();

    int priority = getPriority(thread);
    if (priority == priorityMaximum)
        return false;

    setPriority(thread, priority+1);

    Machine.interrupt().restore(intStatus);
    return true;
    }

    public boolean decreasePriority() {
    boolean intStatus = Machine.interrupt().disable();
               
    KThread thread = KThread.currentThread();

    int priority = getPriority(thread);
    if (priority == priorityMinimum)
        return false;

    setPriority(thread, priority-1);

    Machine.interrupt().restore(intStatus);
    return true;
    }

    public static final int priorityDefault = 1;
    public static final int priorityMinimum = 1;
    public static final int priorityMaximum = Integer.MAX_VALUE;    

    /**
     * Return the scheduling state of the specified thread.
     *
     * @param   thread  the thread whose scheduling state to return.
     * @return  the scheduling state of the specified thread.
     */
    protected LotteryThreadState getLotteryThreadState(KThread thread) {
    if (thread.schedulingState == null)
        thread.schedulingState = new LotteryThreadState(thread);

    return (LotteryThreadState) thread.schedulingState;
    }

    /**
     * A <tt>ThreadQueue</tt> that sorts threads by priority.
     */
    protected class LotteryQueue extends ThreadQueue {

        LotteryQueue(boolean transferPriority) {
            this.transferPriority = transferPriority;
            this.threadStateList = new ArrayList<LotteryThreadState>();
        }

        public int totalTickets() {
            int total = 0;
            for (LotteryThreadState state : threadStateList)
                total += state.getEffectivePriority();
            return total;
        }

        public void waitForAccess(KThread thread) {
            Lib.assertTrue(Machine.interrupt().disabled());
            getLotteryThreadState(thread).waitForAccess(this);
        }

        public void acquire(KThread thread) {
            Lib.assertTrue(Machine.interrupt().disabled());
            getLotteryThreadState(thread).acquire(this);
        }

        public KThread nextThread() {
            Lib.assertTrue(Machine.interrupt().disabled());
            LotteryThreadState nextLotteryThreadState = pickNextThread();
            if (nextLotteryThreadState == null)
                return null;
            Lib.assertTrue(this.threadStateList.remove(nextLotteryThreadState));
            nextLotteryThreadState.acquire(this);
            return nextLotteryThreadState.thread;
        }

        /**
         * Return the next thread that <tt>nextThread()</tt> would return,
         * without modifying the state of this queue.
         *
         * @return  the next thread that <tt>nextThread()</tt> would
         *      return.
         */
        protected LotteryThreadState pickNextThread() {
            LotteryThreadState ret = null;
            if (!threadStateList.isEmpty()) {
                int sum = 0;
                for (int i = 0; i < threadStateList.size(); i++)
                    sum += threadStateList.get(i).getEffectivePriority();
                int choice = (new Random()).nextInt(sum);
                for (int i = 0; i < threadStateList.size(); i++) {
                    choice -= threadStateList.get(i).getEffectivePriority();
                    if (choice < 0) {
                        ret = threadStateList.get(i);
                        break;
                    }
                }
            }
            return ret;
        }
        
        public void print() {
            Lib.assertTrue(Machine.interrupt().disabled());
        }

        /**
         * <tt>true</tt> if this queue should transfer priority from waiting
         * threads to the owning thread.
         */
        public boolean transferPriority;

        protected ArrayList<LotteryThreadState> threadStateList;

        protected KThread acquirer;
    }

    /**
     * The scheduling state of a thread. This should include the thread's
     * priority, its effective priority, any objects it owns, and the queue
     * it's waiting for, if any.
     *
     * @see nachos.threads.KThread#schedulingState
     */

    protected int numCreatedLotteryThreadState = 0;

    protected class LotteryThreadState {
        /**
         * Allocate a new <tt>LotteryThreadState</tt> object and associate it with the
         * specified thread.
         *
         * @param   thread  the thread this state belongs to.
         */
        public LotteryThreadState(KThread thread) {
            this.thread = thread;
            this.id = numCreatedLotteryThreadState++;
            this.acquiredList = new LinkedList<LotteryQueue>();
            setPriority(priorityDefault);
        }

        /**
         * Return the priority of the associated thread.
         *
         * @return  the priority of the associated thread.
         */
        public int getPriority() {
            return priority;
        }

        /**
         * Return the effective priority of the associated thread.
         *
         * @return  the effective priority of the associated thread.
         */
        public int getEffectivePriority() {
            return effectivePriority;
        }

        /**
         * Set the priority of the associated thread to the specified value.
         *
         * @param   priority    the new priority.
         */
        public void setPriority(int priority) {
            if (this.priority == priority)
                return;
            this.priority = priority;
            this.updateEffectivePriority();
        }

        /**
         * Called when <tt>waitForAccess(thread)</tt> (where <tt>thread</tt> is
         * the associated thread) is invoked on the specified priority queue.
         * The associated thread is therefore waiting for access to the
         * resource guarded by <tt>waitQueue</tt>. This method is only called
         * if the associated thread cannot immediately obtain access.
         *
         * @param   waitQueue   the queue that the associated thread is
         *              now waiting on.
         *
         * @see nachos.threads.ThreadQueue#waitForAccess
         */
        public void waitForAccess(LotteryQueue waitQueue) {
            Lib.assertTrue(this.waiting == null);
            this.waiting = waitQueue;
            this.lastWait = Machine.timer().getTime();
            waitQueue.threadStateList.add(this);
            if (waitQueue.acquirer != null)
                getLotteryThreadState(waitQueue.acquirer).updateEffectivePriority();
        }

        /**
         * Called when the associated thread has acquired access to whatever is
         * guarded by <tt>waitQueue</tt>. This can occur either as a result of
         * <tt>acquire(thread)</tt> being invoked on <tt>waitQueue</tt> (where
         * <tt>thread</tt> is the associated thread), or as a result of
         * <tt>nextThread()</tt> being invoked on <tt>waitQueue</tt>.
         *
         * @see nachos.threads.ThreadQueue#acquire
         * @see nachos.threads.ThreadQueue#nextThread
         */
        public void acquire(LotteryQueue waitQueue) {
            if (waitQueue.acquirer != null) {
                getLotteryThreadState(waitQueue.acquirer).release(waitQueue);
            }
            waitQueue.acquirer = this.thread;
            this.acquiredList.add(waitQueue);
            if (this.waiting == waitQueue)
                this.waiting = null;
            this.updateEffectivePriority();
        }

        public void release(LotteryQueue waitQueue) {
            Lib.assertTrue(waitQueue.acquirer == this.thread);
            waitQueue.acquirer = null;
            Lib.assertTrue(this.acquiredList.remove(waitQueue));
            this.updateEffectivePriority();
        }

        /** The thread with which this object is associated. */    
        protected KThread thread;
        /** The priority of the associated thread. */
        protected int priority;

        protected int effectivePriority;

        protected LotteryQueue waiting;

        protected LinkedList<LotteryQueue> acquiredList;

        protected boolean inPath;

        protected void updateEffectivePriority() {
            if (this.inPath)
                return;
            int newEffectivePriority = priority;
            for (LotteryQueue res : acquiredList)
                if (res.transferPriority) {
                    newEffectivePriority += res.totalTickets();
                }
            if (newEffectivePriority != effectivePriority) {
                effectivePriority = newEffectivePriority;
                if (this.waiting != null && this.waiting.acquirer != null) {
                    this.inPath = true;
                    getLotteryThreadState(this.waiting.acquirer).updateEffectivePriority();
                    this.inPath = false;
                }
            }
        }

        protected long lastWait;

        protected int id;
    }
}