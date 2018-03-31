package nachos.threads;
import nachos.ag.BoatGrader;

public class Boat
{
    static BoatGrader bg;

    static boolean hasLeader1;
    static boolean hasLeader2;
    static Lock lock;
    static Condition cond1;
    static Condition cond2;
    static Condition condP;
    static Condition condIdle;
    static Communicator comm;
    static int numRider = 0;

    public static void selfTest()
    {
	BoatGrader b = new BoatGrader();
	
	System.out.println("\n ***Testing Boats with only 2 children***");
	begin(10, 20, b);

//	System.out.println("\n ***Testing Boats with 2 children, 1 adult***");
//  	begin(1, 2, b);

//  	System.out.println("\n ***Testing Boats with 3 children, 3 adults***");
//  	begin(3, 3, b);
    }

    public static void begin( int adults, int children, BoatGrader b )
    {
	// Store the externally generated autograder in a class
	// variable to be accessible by children.
	bg = b;

	// Instantiate global variables here
	
	// Create threads here. See section 3.4 of the Nachos for Java
	// Walkthrough linked from the projects page.

	hasLeader1 = hasLeader2 = false;
	lock = new Lock();
	cond1 = new Condition(lock);
	cond2 = new Condition(lock);
	condP = new Condition(lock);
	condIdle = new Condition(lock);
	comm = new Communicator();

	Runnable adult = new Runnable() {
		public void run() {
			AdultItinerary();
		}
	};
	
	Runnable child = new Runnable() {
		public void run() {
			ChildItinerary();
		}
	};

	for (int i = 0; i < children; i++)
		new KThread(child).setName("Child " + (i + 1)).fork();

	for (int i = 0; i < adults; i++)
		new KThread(adult).setName("Adult " + (i + 1)).fork();

	while (comm.listen() != adults + children);
    }

    static void Rider(boolean isAdult) {
    	numRider++;
    	condIdle.wake();
    	condP.sleep();
    	if (isAdult)
    		bg.AdultRowToMolokai();
    	else
    		bg.ChildRowToMolokai();
    	numRider--;
    	cond2.wake();
    	lock.release();
    }

    static void Leader1() {
    	int total = 2;
    	while (true) {
    		cond1.sleep();
    		bg.ChildRowToMolokai();
    		cond2.wake();
    		cond1.sleep();
    		while (numRider == 0) {
    			comm.speak(total);
    			condIdle.sleep();
    		}
    		total++;
    		bg.ChildRowToOahu();
    		condP.wake();
    	}
    }

    static void Leader2() {
    	while (true) {
    		cond1.wake();
    		cond2.sleep();
    		bg.ChildRideToMolokai();
    		cond1.wake();
    		cond2.sleep();
    		bg.ChildRowToOahu();
    	}
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
	   	lock.acquire();
		Rider(true);
    }

    static void ChildItinerary()
    {
	bg.initializeChild(); //Required for autograder interface. Must be the first thing called.
	//DO NOT PUT ANYTHING ABOVE THIS LINE. 
		lock.acquire();
		if (!hasLeader1) {
			hasLeader1 = true;
			Leader1();
		} else if (!hasLeader2) {
			hasLeader2 = true;
			Leader2();
		} else
			Rider(false);
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
