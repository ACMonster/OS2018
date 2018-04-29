package nachos.threads;

import nachos.machine.*;

import java.util.*;

/**
 * A scheduler that chooses threads based on their priorities.
 *
 * <p>
 * A priority scheduler associates a priority with each thread. The next thread
 * to be dequeued is always a thread with priority no less than any other
 * waiting thread's priority. Like a round-robin scheduler, the thread that is
 * dequeued is, among all the threads of the same (highest) priority, the
 * thread that has been waiting longest.
 *
 * <p>
 * Essentially, a priority scheduler gives access in a round-robin fassion to
 * all the highest-priority threads, and ignores all other threads. This has
 * the potential to
 * starve a thread if there's always a thread waiting with higher priority.
 *
 * <p>
 * A priority scheduler must partially solve the priority inversion problem; in
 * particular, priority must be donated through locks, and through joins.
 */
public class PriorityScheduler extends Scheduler {
    /**
     * Allocate a new priority scheduler.
     */
    public PriorityScheduler() {
    }
    
    /**
     * Allocate a new priority thread queue.
     *
     * @param	transferPriority	<tt>true</tt> if this queue should
     *					transfer priority from waiting threads
     *					to the owning thread.
     * @return	a new priority thread queue.
     */
    public ThreadQueue newThreadQueue(boolean transferPriority) {
	return new PriorityQueue(transferPriority);
    }

    public int getPriority(KThread thread) {
	Lib.assertTrue(Machine.interrupt().disabled());
		       
	return getThreadState(thread).getPriority();
    }

    public int getEffectivePriority(KThread thread) {
	Lib.assertTrue(Machine.interrupt().disabled());
		       
	return getThreadState(thread).getEffectivePriority();
    }

    public void setPriority(KThread thread, int priority) {
	Lib.assertTrue(Machine.interrupt().disabled());
		       
	Lib.assertTrue(priority >= priorityMinimum &&
		   priority <= priorityMaximum);
	
	getThreadState(thread).setPriority(priority);
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
    public static final int priorityMinimum = 0;
    /**
     * The maximum priority that a thread can have. Do not change this value.
     */
    public static final int priorityMaximum = 7;    

    /**
     * Return the scheduling state of the specified thread.
     *
     * @param	thread	the thread whose scheduling state to return.
     * @return	the scheduling state of the specified thread.
     */
    protected ThreadState getThreadState(KThread thread) {
	if (thread.schedulingState == null)
	    thread.schedulingState = new ThreadState(thread);

	return (ThreadState) thread.schedulingState;
    }

    /**
     * A <tt>ThreadQueue</tt> that sorts threads by priority.
     */
    protected class PriorityQueue extends ThreadQueue {
	PriorityQueue(boolean transferPriority) {
	    this.transferPriority = transferPriority;
	    // this.heapPriority = new TreeSet<ThreadState>(new PriorityComparator());
		this.heapEffectivePriority = new TreeSet<ThreadState>(new EffectivePriorityComparator());
		// this.waitingList = new LinkedList<KThread>();
	}

	public void waitForAccess(KThread thread) {
	    Lib.assertTrue(Machine.interrupt().disabled());
	    getThreadState(thread).waitForAccess(this);
	}

	public void acquire(KThread thread) {
	    Lib.assertTrue(Machine.interrupt().disabled());
	    getThreadState(thread).acquire(this);
	}

	public KThread nextThread() {
	    Lib.assertTrue(Machine.interrupt().disabled());
	    // implement me
		ThreadState nextThreadState = pickNextThread();
	    if (nextThreadState == null)
	    	return null;
		// getThreadState(this.acquirer).release(this);
	    Lib.assertTrue(this.heapEffectivePriority.remove(nextThreadState));
		nextThreadState.acquire(this);
		return nextThreadState.thread;
	}

	/**
	 * Return the next thread that <tt>nextThread()</tt> would return,
	 * without modifying the state of this queue.
	 *
	 * @return	the next thread that <tt>nextThread()</tt> would
	 *		return.
	 */
	protected ThreadState pickNextThread() {
	    // implement me
	    ThreadState ret = null;
	    if (!heapEffectivePriority.isEmpty())
	    	ret = heapEffectivePriority.last();
		return ret;
	}
	
	public void print() {
	    Lib.assertTrue(Machine.interrupt().disabled());
	    // implement me (if you want)
	}

	/**
	 * <tt>true</tt> if this queue should transfer priority from waiting
	 * threads to the owning thread.
	 */
	public boolean transferPriority;

	protected KThread acquirer;

	// protected TreeSet<ThreadState> heapPriority;

	protected TreeSet<ThreadState> heapEffectivePriority;

	// protected LinkedList<KThread> waitingList;
    }

    /**
     * The scheduling state of a thread. This should include the thread's
     * priority, its effective priority, any objects it owns, and the queue
     * it's waiting for, if any.
     *
     * @see	nachos.threads.KThread#schedulingState
     */

    protected int numCreatedThreadState = 0;

    protected class ThreadState {
	/**
	 * Allocate a new <tt>ThreadState</tt> object and associate it with the
	 * specified thread.
	 *
	 * @param	thread	the thread this state belongs to.
	 */
	public ThreadState(KThread thread) {
	    this.thread = thread;
	    this.id = numCreatedThreadState++;
	    this.acquiredList = new LinkedList<PriorityQueue>();
	    setPriority(priorityDefault);
	}

	/**
	 * Return the priority of the associated thread.
	 *
	 * @return	the priority of the associated thread.
	 */
	public int getPriority() {
	    return priority;
	}

	/**
	 * Return the effective priority of the associated thread.
	 *
	 * @return	the effective priority of the associated thread.
	 */
	public int getEffectivePriority() {
	    // implement me
	    return effectivePriority;
	}

	/**
	 * Set the priority of the associated thread to the specified value.
	 *
	 * @param	priority	the new priority.
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
	 * @param	waitQueue	the queue that the associated thread is
	 *				now waiting on.
	 *
	 * @see	nachos.threads.ThreadQueue#waitForAccess
	 */
	public void waitForAccess(PriorityQueue waitQueue) {
	    // implement me
		Lib.assertTrue(this.waiting == null);
		this.waiting = waitQueue;
		this.lastWait = Machine.timer().getTime();
		waitQueue.heapEffectivePriority.add(this);
		if (waitQueue.acquirer != null)
			getThreadState(waitQueue.acquirer).updateEffectivePriority();
	}

	/**
	 * Called when the associated thread has acquired access to whatever is
	 * guarded by <tt>waitQueue</tt>. This can occur either as a result of
	 * <tt>acquire(thread)</tt> being invoked on <tt>waitQueue</tt> (where
	 * <tt>thread</tt> is the associated thread), or as a result of
	 * <tt>nextThread()</tt> being invoked on <tt>waitQueue</tt>.
	 *
	 * @see	nachos.threads.ThreadQueue#acquire
	 * @see	nachos.threads.ThreadQueue#nextThread
	 */
	public void acquire(PriorityQueue waitQueue) {
	    // implement me
		// Lib.assertTrue(waitQueue.acquirer == null);
		if (waitQueue.acquirer != null) {
			getThreadState(waitQueue.acquirer).release(waitQueue);
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
		for (PriorityQueue res : acquiredList) if (res.transferPriority && !res.heapEffectivePriority.isEmpty())
				if (res.heapEffectivePriority.last().effectivePriority > newEffectivePriority)
					newEffectivePriority = res.heapEffectivePriority.last().effectivePriority;
		if (newEffectivePriority != effectivePriority) {
			if (this.waiting != null)
				Lib.assertTrue(this.waiting.heapEffectivePriority.remove(this));
			effectivePriority = newEffectivePriority;
			if (this.waiting != null) {
				this.waiting.heapEffectivePriority.add(this);
				Lib.assertTrue(this.waiting.acquirer != null);
				this.inPath = true;
				getThreadState(this.waiting.acquirer).updateEffectivePriority();
				this.inPath = false;
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
		((PriorityScheduler)ThreadedKernel.scheduler).getThreadState(kt1).setPriority(1);
		((PriorityScheduler)ThreadedKernel.scheduler).getThreadState(kt2).setPriority(2);
		((PriorityScheduler)ThreadedKernel.scheduler).getThreadState(kt3).setPriority(3);
		((PriorityScheduler)ThreadedKernel.scheduler).getThreadState(kt4).setPriority(3);
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
