package nachos.threads;

import nachos.machine.*;

import java.util.LinkedList;

/**
 * An implementation of condition variables that disables interrupt()s for
 * synchronization.
 *
 * <p>
 * You must implement this.
 *
 * @see	nachos.threads.Condition
 */
public class Condition2 {
    /**
     * Allocate a new condition variable.
     *
     * @param	conditionLock	the lock associated with this condition
     *				variable. The current thread must hold this
     *				lock whenever it uses <tt>sleep()</tt>,
     *				<tt>wake()</tt>, or <tt>wakeAll()</tt>.
     */

    private LinkedList<KThread> waitList;

    public Condition2(Lock conditionLock) {
        this.conditionLock = conditionLock;
        waitList = new LinkedList<KThread>();
    }

    /**
     * Atomically release the associated lock and go to sleep on this condition
     * variable until another thread wakes it using <tt>wake()</tt>. The
     * current thread must hold the associated lock. The thread will
     * automatically reacquire the lock before <tt>sleep()</tt> returns.
     */
    public void sleep() {
	Lib.assertTrue(conditionLock.isHeldByCurrentThread());

	boolean intStatus = Machine.interrupt().disable();

    waitList.add(KThread.currentThread());

	conditionLock.release();
    KThread.sleep();
	conditionLock.acquire();

	Machine.interrupt().restore(intStatus);
    }

    /**
     * Wake up at most one thread sleeping on this condition variable. The
     * current thread must hold the associated lock.
     */
    public void wake() {
        Lib.assertTrue(conditionLock.isHeldByCurrentThread());

        boolean intStatus = Machine.interrupt().disable();

        if (!waitList.isEmpty())
            waitList.removeFirst().ready();

        Machine.interrupt().restore(intStatus);
    }

    /**
     * Wake up all threads sleeping on this condition variable. The current
     * thread must hold the associated lock.
     */
    public void wakeAll() {
	    Lib.assertTrue(conditionLock.isHeldByCurrentThread());

        boolean intStatus = Machine.interrupt().disable();

        while (!waitList.isEmpty())
            waitList.remove().ready();

        Machine.interrupt().restore(intStatus);
    }

    private Lock conditionLock;

    public static void selfTest() {
        System.out.println();
        // Only behaves as desired when using a RoundRobinScheduler.
        Lib.debug('t', "Enter Condition2.selfTest");
        Lib.debug('t', "PingTest(20), PingTest(21), PingTest(22) sleep on the same condition variable");
        final Lock mu = new Lock();
        final Condition2 cv = new Condition2(mu);
        class CVPingTest extends KThread.PingTest {
            CVPingTest(int i) {
                super(i);
            }

            @Override
            public void run() {
                mu.acquire();
                cv.sleep();
                mu.release();
                super.run();
            }
        }
        KThread kt1 = new KThread(new CVPingTest(20));
        KThread kt2 = new KThread(new CVPingTest(21));
        KThread kt3 = new KThread(new CVPingTest(22));
        kt1.fork();
        kt2.fork();
        kt3.fork();
        KThread.yield();
        mu.acquire();
        cv.wake();
        mu.release();
        kt1.join();
        mu.acquire();
        cv.wakeAll();
        mu.release();
        KThread.yield();
        kt2.join();
        kt3.join();
    }
}
