package mapreduce1;
import java.util.Random;

public class Configure {
	// Static info
	static char[] ltable = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '-', '*', '/' };
	static int chromoLen = 5;
	static double crossRate = .7;
	static double mutRate = .001;
	static Random rand = new Random();
	static int poolSize = 40; // Must be even
}
