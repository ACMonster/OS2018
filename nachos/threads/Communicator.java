package nachos.threads;

import nachos.machine.*;

/**
 * A <i>communicator</i> allows threads to synchronously exchange 32-bit
 * messages. Multiple threads can be waiting to <i>speak</i>,
 * and multiple threads can be waiting to <i>listen</i>. But there should never
 * be a time when both a speaker and a listener are waiting, because the two
 * threads can be paired off at this point.
 */
public class Communicator {
    /**
     * Allocate a new communicator.
     */

    private Lock mu;
    private Condition2 waitingSpeakers, waitingListeners, currentSpeaker;
    private int reg;
    private boolean isSet, isRetrieved;

    public Communicator() {
        mu = new Lock();
        waitingListeners = new Condition2(mu);
        waitingSpeakers = new Condition2(mu);
        currentSpeaker = new Condition2(mu);
        isSet = false;
        isRetrieved = false;
    }

    /**
     * Wait for a thread to listen through this communicator, and then transfer
     * <i>word</i> to the listener.
     *
     * <p>
     * Does not return until this thread is paired up with a listening thread.
     * Exactly one listener should receive <i>word</i>.
     *
     * @param	word	the integer to transfer.
     */
    public void speak(int word) {
        mu.acquire();
        while (isSet)
            waitingSpeakers.sleep();
        isSet = true;
        isRetrieved = false;
        reg = word;
        waitingListeners.wake();
        while (!isRetrieved)
            currentSpeaker.sleep();
        isSet = false;
        waitingSpeakers.wake();
        mu.release();
    }

    /**
     * Wait for a thread to speak through this communicator, and then return
     * the <i>word</i> that thread passed to <tt>speak()</tt>.
     *
     * @return	the integer transferred.
     */    
    public int listen() {
        mu.acquire();
        while (!isSet || isRetrieved)
            waitingListeners.sleep();
        int ret = reg;
        isRetrieved = true;
        currentSpeaker.wake();
        mu.release();
        return ret;
    }

    public static void selfTest() {
        System.out.println();
        final Communicator c = new Communicator();
        class CommunicatorTest implements Runnable {
            boolean isSpeaker;
            int word;
            int id;
            long sleep;
            CommunicatorTest(boolean isSpeaker, int word, int id, long sleep) {
                this.isSpeaker = isSpeaker;
                this.word = word;
                this.id = id;
                this.sleep = sleep;
            }

            @Override
            public void run() {
                if (isSpeaker) {
                    ThreadedKernel.alarm.waitUntil(sleep);
                    System.out.println("*** thread " + id + " speaks " + word);
                    c.speak(word);
                }
                else {
                    ThreadedKernel.alarm.waitUntil(sleep);
                    word = c.listen();
                    System.out.println("*** thread " + id + " hears " + word);
                }
            }
        }
        KThread[] kt = new KThread[8];
        for(int i = 0; i < 4; i++) {
            kt[i] = new KThread(new CommunicatorTest(true, i, 40 + i, 200 + i * 700));
            kt[i].fork();
        }
        for(int i = 4; i < 8; i++) {
            kt[i] = new KThread(new CommunicatorTest(false, 0, 40 + i, 2000 - i * 100));
            kt[i].fork();
        }
        KThread.yield();
        for(int i = 0; i < 8; i++)
            kt[i].join();
    }
}
