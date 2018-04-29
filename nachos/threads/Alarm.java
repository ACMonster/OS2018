package nachos.threads;

import nachos.machine.*;

import java.util.TreeSet;

/**
 * Uses the hardware timer to provide preemption, and to allow threads to sleep
 * until a certain time.
 */
public class Alarm {
    /**
     * Allocate a new Alarm. Set the machine's timer interrupt handler to this
     * alarm's callback.
     *
     * <p><b>Note</b>: Nachos will not function correctly with more than one
     * alarm.
     */
    private Alarm() {
        waiting = new TreeSet<WaitingKThread>();
	    Machine.timer().setInterruptHandler(new Runnable() {
	    	public void run() { timerInterrupt(); }
        });
    }

    private static Alarm alarmInstance;

    public static Alarm newAlarm() {
        Lib.assertTrue(alarmInstance == null);
        alarmInstance = new Alarm();
        return alarmInstance;
    }

    public static Alarm getAlarm() {
        Lib.assertTrue(alarmInstance != null);
        return alarmInstance;
    }

    private TreeSet<WaitingKThread> waiting;

    /**
     * The timer interrupt handler. This is called by the machine's timer
     * periodically (approximately every 500 clock ticks). Causes the current
     * thread to yield, forcing a context switch if there is another thread
     * that should be run.
     */
    public void timerInterrupt() {
        boolean intStatus = Machine.interrupt().disable();

        while (!waiting.isEmpty() && waiting.first().waitTime <= Machine.timer().getTime())
            waiting.pollFirst().kThread.ready();

        Machine.interrupt().restore(intStatus);
    }

    /**
     * Put the current thread to sleep for at least <i>x</i> ticks,
     * waking it up in the timer interrupt handler. The thread must be
     * woken up (placed in the scheduler ready set) during the first timer
     * interrupt where
     *
     * <p><blockquote>
     * (current time) >= (WaitUntil called time)+(x)
     * </blockquote>
     *
     * @param	x	the minimum number of clock ticks to wait.
     *
     * @see	nachos.machine.Timer#getTime()
     */
    public void waitUntil(long x) {
	    // implemented
	    long wakeTime = Machine.timer().getTime() + x;
	    boolean intStatus = Machine.interrupt().disable();

	    waiting.add(new WaitingKThread(wakeTime, KThread.currentThread()));
	    KThread.sleep();

	    Machine.interrupt().restore(intStatus);
    }

    private long numWaitingKThreadCreated = 0;

    private class WaitingKThread implements Comparable {
        long waitTime;
        KThread kThread;
        //Probably unnecessary unique identity number: difficult to imagine a KThread to have more than one list term in the alarm waiting list
        private long id;

        WaitingKThread(long waitTime, KThread kThread) {
            this.waitTime = waitTime;
            this.kThread = kThread;
            this.id = numWaitingKThreadCreated++;
        }

        @Override
        public int compareTo(Object o) {
            WaitingKThread another = (WaitingKThread)o;
            if (waitTime < another.waitTime)
                return -1;
            if (waitTime > another.waitTime)
                return 1;
            if (id < another.id)
                return -1;
            if (id > another.id)
                return 1;
            return 0;
        }
    }

    public static void selfTest() {

        System.out.println();

        final Alarm alarm = getAlarm();
        class WaitingPingTest extends KThread.PingTest {
            private long x1, x2;
            private int i;
            WaitingPingTest(int i, long x1, long x2) {
                super(i);
                this.i = i;
                this.x1 = x1;
                this.x2 = x2;
            }

            @Override
            public void run() {
                super.run();
                System.out.println("*** thread " + i + " waits until " + x1);
                alarm.waitUntil(x1);
                System.out.println("*** thread " + i + " running again");
                super.run();
                System.out.println("*** thread " + i + " waits until " + x2);
                alarm.waitUntil(x2);
                System.out.println("*** thread " + i + " running again");
                super.run();
            }
        }

        KThread kt1 = new KThread(new WaitingPingTest(30, 600, 700));
        KThread kt2 = new KThread(new WaitingPingTest(31, 1200, 100));
        kt1.fork();
        kt2.fork();
        KThread.yield();
        kt1.join();
        kt2.join();
    }
}
