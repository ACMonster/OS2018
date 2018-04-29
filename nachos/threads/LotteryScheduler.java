package nachos.threads;

import nachos.machine.*;

import java.util.LinkedList;
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
    // implement me
    return new PriorityQueue(transferPriority);
    }

    public int getPriority(KThread thread) {
    Lib.assertTrue(Machine.interrupt().disabled());
               
    return getNewThreadState(thread).getPriority();
    }

    public int getEffectivePriority(KThread thread) {
    Lib.assertTrue(Machine.interrupt().disabled());
               
    return getNewThreadState(thread).getEffectivePriority();
    }

    public void setPriority(KThread thread, int priority) {
    Lib.assertTrue(Machine.interrupt().disabled());
               
    Lib.assertTrue(priority >= priorityMinimum &&
           priority <= priorityMaximum);
    
    getNewThreadState(thread).setPriority(priority);
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

    /**
     * The default priority for a new thread. Do not change this value.
     */
    public static final int priorityDefault = 1;
    /**
     * The minimum priority that a thread can have. Do not change this value.
     */
    public static final int priorityMinimum = 1;
    /**
     * The maximum priority that a thread can have. Do not change this value.
     */
    public static final int priorityMaximum = 1 << 31 - 1;    

    /**
     * Return the scheduling state of the specified thread.
     *
     * @param   thread  the thread whose scheduling state to return.
     * @return  the scheduling state of the specified thread.
     */
    protected NewThreadState getNewThreadState(KThread thread) {
    if (thread.schedulingState == null) 
        thread.schedulingState = new NewThreadState(thread);

    return (NewThreadState) thread.schedulingState;
    }

    /**
     * A <tt>ThreadQueue</tt> that sorts threads by priority.
     */
    protected class PriorityQueue extends ThreadQueue {
    PriorityQueue(boolean transferPriority) {
        this.transferPriority = transferPriority;
        // this.heapPriority = new TreeSet<ThreadState>(new PriorityComparator());
        //this.heapEffectivePriority = new TreeSet<ThreadState>(new EffectivePriorityComparator());
        this.waitingList = new LinkedList<KThread>();
    }

    public void waitForAccess(KThread thread) {
        Lib.assertTrue(Machine.interrupt().disabled());
        getNewThreadState(thread).waitForAccess(this);
    }

    public void acquire(KThread thread) {
        Lib.assertTrue(Machine.interrupt().disabled());
        getNewThreadState(thread).acquire(this);
    }

    public KThread nextThread() {
        Lib.assertTrue(Machine.interrupt().disabled());
        // implement me
        NewThreadState nextThreadState = pickNextThread();
        if (nextThreadState == null)
            return null;
        // getNewThreadState(this.acquirer).release(this);
        Lib.assertTrue(this.waitingList.remove(nextThreadState.thread));
        nextThreadState.acquire(this);
        //print();
        return nextThreadState.thread;
    }

    /**
     * Return the next thread that <tt>nextThread()</tt> would return,
     * without modifying the state of this queue.
     *
     * @return  the next thread that <tt>nextThread()</tt> would
     *      return.
     */
    protected NewThreadState pickNextThread() {
        // implement me
        NewThreadState ret = null;
        //boolean f = waitingList.remove(KThread.currentThread);
        int totalTickets = getTotalTickets();
        if (totalTickets > 0) {
            int chooseTickets = (new Random()).nextInt(totalTickets) + 1;
            for (KThread thread : waitingList) {
                chooseTickets -= getNewThreadState(thread).getEffectivePriority();
                if (chooseTickets <= 0) {
                    ret = getNewThreadState(thread);
                    break;
                }
            }
        }
        //if (f) waitingList.add(KThread.currentThread);
        return ret;
    }

    public int getTotalTickets() {
        int totalTickets = 0; 
        for (KThread thread : waitingList) totalTickets += getNewThreadState(thread).getEffectivePriority();
        return totalTickets;
    }
    
    public void print() {
        Lib.assertTrue(Machine.interrupt().disabled());
        // implement me (if you want)
        for (KThread thread : waitingList)
            System.out.println(thread);
    }

    /**
     * <tt>true</tt> if this queue should transfer priority from waiting
     * threads to the owning thread.
     */
    public boolean transferPriority;

    protected KThread acquirer;

    // protected TreeSet<ThreadState> heapPriority;

    //protected TreeSet<ThreadState> heapEffectivePriority;

    protected LinkedList<KThread> waitingList;
    }

    /**
     * The scheduling state of a thread. This should include the thread's
     * priority, its effective priority, any objects it owns, and the queue
     * it's waiting for, if any.
     *
     * @see nachos.threads.KThread#schedulingState
     */

    protected int numCreatedThreadState = 0;

    protected class NewThreadState {
    /**
     * Allocate a new <tt>ThreadState</tt> object and associate it with the
     * specified thread.
     *
     * @param   thread  the thread this state belongs to.
     */
    public NewThreadState(KThread thread) {
        this.thread = thread;
        this.id = numCreatedThreadState++;
        this.acquiredList = new LinkedList<PriorityQueue>();
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
        // implement me
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
        
        // implement me
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
    public void waitForAccess(PriorityQueue waitQueue) {
        // implement me
        Lib.assertTrue(this.waiting == null);
        this.waiting = waitQueue;
        this.lastWait = Machine.timer().getTime();
        waitQueue.waitingList.add(this.thread);
        if (waitQueue.acquirer != null)
            getNewThreadState(waitQueue.acquirer).updateEffectivePriority();
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
    public void acquire(PriorityQueue waitQueue) {
        // implement me
        // Lib.assertTrue(waitQueue.acquirer == null);
        if (waitQueue.acquirer != null) {
            getNewThreadState(waitQueue.acquirer).release(waitQueue);
        }
        waitQueue.acquirer = this.thread;
        this.acquiredList.add(waitQueue);
        if (this.waiting == waitQueue)
            this.waiting = null;
        this.updateEffectivePriority();
    }

    public void release(PriorityQueue waitQueue) {
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

    protected PriorityQueue waiting;

    protected LinkedList<PriorityQueue> acquiredList;

    protected boolean inPath;

    protected void updateEffectivePriority() {
        if (this.inPath) return;
        int newEffectivePriority = priority;
        for (PriorityQueue res : this.acquiredList) if (res.transferPriority && !res.waitingList.isEmpty())
                newEffectivePriority += res.getTotalTickets();
        if (newEffectivePriority != effectivePriority) {
            if (this.waiting != null)
                Lib.assertTrue(this.waiting.waitingList.remove(this.thread));
            effectivePriority = newEffectivePriority;
            if (this.waiting != null) {
                this.waiting.waitingList.add(this.thread);
                //Lib.assertTrue(this.waiting.acquirer != null);
                if (this.waiting.acquirer != null) {
                    this.inPath = true;
                    getNewThreadState(this.waiting.acquirer).updateEffectivePriority();
                    this.inPath = false;
                }
            }
        }
    }

    protected long lastWait;

    protected int id;
    }

    /* protected class PriorityComparator implements Comparator<ThreadState> {
        @Override
        public int compare(ThreadState o1, ThreadState o2) {
            if (o1.priority < o2.priority)
                return -1;
            if (o1.priority > o2.priority)
                return 1;
            if (o1.id < o2.id)
                return -1;
            if (o1.id > o2.id)
                return 1;
            return 0;
        }
    } */
    /*
    protected class EffectivePriorityComparator implements Comparator<ThreadState> {
        @Override
        public int compare(ThreadState o1, ThreadState o2) {
            if (o1.effectivePriority < o2.effectivePriority)
                return -1;
            if (o1.effectivePriority > o2.effectivePriority)
                return 1;
            if (o1.lastWait < o2.lastWait)
                return 1;
            if (o1.lastWait > o2.lastWait)
                return -1;
            if (o1.id < o2.id)
                return -1;
            if (o1.id > o2.id)
                return 1;
            return 0;
        }
    }
    */

    public static void selfTest() {
        System.out.println();
        class JoinPingTest extends KThread.PingTest {
            KThread joiner;
            JoinPingTest(int i, KThread joiner) {
                super(i);
                this.joiner = joiner;
            }

            @Override
            public void run() {
                if (joiner != null) joiner.join();
                super.run();
            }
        }
        KThread kt1 = new KThread(new JoinPingTest(50, null));
        KThread kt2 = new KThread(new JoinPingTest(51, kt1));
        KThread kt3 = new KThread(new JoinPingTest(52, kt2));
        KThread kt4 = new KThread(new JoinPingTest(53, null));
        ((LotteryScheduler)ThreadedKernel.scheduler).getNewThreadState(kt1).setPriority(1);
        ((LotteryScheduler)ThreadedKernel.scheduler).getNewThreadState(kt2).setPriority(2);
        ((LotteryScheduler)ThreadedKernel.scheduler).getNewThreadState(kt3).setPriority(3);
        ((LotteryScheduler)ThreadedKernel.scheduler).getNewThreadState(kt4).setPriority(3);
        kt1.fork();
        kt2.fork();
        kt3.fork();
        kt4.fork();
        KThread.yield();
        kt1.join();
        kt2.join();
        kt3.join();
        kt4.join();
    }
}
