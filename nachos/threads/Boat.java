package nachos.threads;
import nachos.ag.BoatGrader;

import java.util.LinkedList;

public class Boat
{
    static BoatGrader bg;
    
    public static void selfTest()
    {
	BoatGrader b = new BoatGrader();
	
	System.out.println("\n ***Testing Boats with only 2 children***");
	begin(3, 5, b);

//	System.out.println("\n ***Testing Boats with 2 children, 1 adult***");
//  	begin(1, 2, b);

//  	System.out.println("\n ***Testing Boats with 3 children, 3 adults***");
//  	begin(3, 3, b);
    }

    private static boolean boatAtO;
    private static boolean wantsPassenger;
    private static int numChildrenAtO;
    private static int numChildrenAtM;
    private static int numAdultsAtO;
    private static int numAdultsAtM;
    private static Lock mu;
    private static Condition2 cvChildrenAtO;
    private static Condition2 cvChildrenAtM;
    private static Condition2 cvAdultsAtO;
    private static Condition2 cvWantsPassenger;

    public static void begin( int adults, int children, BoatGrader b )
    {
	// Store the externally generated autograder in a class
	// variable to be accessible by children.
	bg = b;

	// Instantiate global variables here

    boatAtO = true;
    wantsPassenger = false;
    numChildrenAtO = children;
    numChildrenAtM = 0;
    numAdultsAtO = adults;
    numAdultsAtM = 0;
    mu = new Lock();
    cvChildrenAtO = new Condition2(mu);
    cvChildrenAtM = new Condition2(mu);
	cvAdultsAtO = new Condition2(mu);
	cvWantsPassenger = new Condition2(mu);

	// Create threads here. See section 3.4 of the Nachos for Java
	// Walkthrough linked from the projects page.

        LinkedList<KThread> list = new LinkedList<KThread>();
	for (int i=0; i < adults; i++) {
        KThread kt = new KThread(new Runnable() {
            @Override
            public void run() {
                AdultItinerary();
            }
        });
        kt.fork();
        list.add(kt);
    }
	for (int i=0; i < children; i++) {
        KThread kt = new KThread(new Runnable() {
            @Override
            public void run() {
                ChildItinerary();
            }
        });
        kt.fork();
        list.add(kt);
    }
    for (KThread kt : list)
        kt.join();

    }

    static void AdultItinerary()
    {
	bg.initializeAdult(); //Required for autograder interface. Must be the first thing called.
	//DO NOT PUT ANYTHING ABOVE THIS LINE. 

	/* This is where you should put your solutions. Make calls
	   to the BoatGrader to show that it is synchronized. For
	   example:
	       bg.AdultRowToMolokai();
	   indicates that an adult has rowed the boat across to Molokai
	*/

	mu.acquire();

	while (!(numChildrenAtM > 0 && boatAtO))
        cvAdultsAtO.sleep();
	bg.AdultRowToMolokai();
    boatAtO = false;
    numAdultsAtO --;
    numAdultsAtM ++;
    if (numAdultsAtO + numChildrenAtO > 0)
        cvChildrenAtM.wake();

	mu.release();
    }

    static void ChildItinerary()
    {
	bg.initializeChild(); //Required for autograder interface. Must be the first thing called.
	//DO NOT PUT ANYTHING ABOVE THIS LINE.
    boolean thisAtO = true;
    mu.acquire();

    for(;;) {
        if (numAdultsAtO + numChildrenAtO == 0) {
            cvChildrenAtM.wakeAll();
            cvAdultsAtO.wakeAll();
            cvChildrenAtO.wakeAll();
            cvWantsPassenger.wakeAll();
            break;
        }
        if (!boatAtO)
            if (!thisAtO) {
                bg.ChildRowToOahu();
                thisAtO = true;
                numChildrenAtM --;
                numChildrenAtO ++;
                boatAtO = true;
                if (numAdultsAtO > 0 && numChildrenAtM > 0) {
                    cvAdultsAtO.wake();
                    cvChildrenAtO.sleep();
                }
            }
            else {
                cvChildrenAtM.wake();
                cvChildrenAtO.sleep();
            }
        else //boat at O
            if (!thisAtO)
                if (numAdultsAtO > 0) {
                    cvAdultsAtO.wake();
                    cvChildrenAtM.sleep();
                }
                else {
                    cvChildrenAtO.wake();
                    cvChildrenAtM.sleep();
                }
            else // this at O
                if (numChildrenAtM > 0 && numAdultsAtO > 0) {
                    cvAdultsAtO.wake();
                    cvChildrenAtO.sleep();
                }
                else
                if (!wantsPassenger)
                    if (numChildrenAtO == 1) {
                        bg.ChildRowToMolokai();
                        boatAtO = false;
                        thisAtO = false;
                        numChildrenAtO --;
                        numChildrenAtM ++;
                    }
                    else {
                        wantsPassenger = true;
                        bg.ChildRowToMolokai();
                        cvChildrenAtO.wake();
                        thisAtO = false;
                        while (wantsPassenger)
                            cvWantsPassenger.sleep();
                   }
                else {
                    wantsPassenger = false;
                    bg.ChildRideToMolokai();
                    thisAtO = false;
                    boatAtO = false;
                    numChildrenAtO -= 2;
                    numChildrenAtM += 2;
                    cvWantsPassenger.wake();
                    cvChildrenAtM.sleep();
                }
    }

    mu.release();
    }

    static void SampleItinerary()
    {
	// Please note that this isn't a valid solution (you can't fit
	// all of them on the boat). Please also note that you may not
	// have a single thread calculate a solution and then just play
	// it back at the autograder -- you will be caught.
	System.out.println("\n ***Everyone piles on the boat and goes to Molokai***");
	bg.AdultRowToMolokai();
	bg.ChildRideToMolokai();
	bg.AdultRideToMolokai();
	bg.ChildRideToMolokai();
    }
    
}
