package mapreduce1;
import java.util.ArrayList;
import java.util.Comparator;

class Chomosone implements Comparable<Chomosone>{
	// The chromo
	StringBuffer chromo = new StringBuffer(Configure.chromoLen * 4);
	public StringBuffer decodeChromo = new StringBuffer(Configure.chromoLen * 4);
	public double score;
	public int total;

	// Constructor that generates a random
	public Chomosone(int target) {

		// Create the full buffer
		for (int y = 0; y < Configure.chromoLen; y++) {
			// What's the current length
			int pos = chromo.length();

			// Generate a random binary integer
			String binString = Integer.toBinaryString(Configure.rand.nextInt(Configure.ltable.length));
			int fillLen = 4 - binString.length();

			// Fill to 4
			for (int x = 0; x < fillLen; x++)
				chromo.append('0');

			// Append the chromo
			chromo.append(binString);

		}

		// Score the new cromo
		scoreChromo(target);
	}

	public Chomosone(StringBuffer chromo) {
		this.chromo = chromo;
	}

	// Decode the string
	public  String decodeChromo() {

		// Create a buffer
		decodeChromo.setLength(0);

		// Loop throught the chromo
		for (int x = 0; x < chromo.length(); x += 4) {
			// Get the
			int idx = Integer.parseInt(chromo.substring(x, x + 4), 2);
			if (idx < Configure.ltable.length)
				decodeChromo.append(Configure.ltable[idx]);
		}

		// Return the string
		return decodeChromo.toString();
	}

	// Scores this chromo
	public final void scoreChromo(int target) {
		total = addUp();
		if (total == target)
			score = 0;
		score = (double) 1 / (target - total);
	}

	// Crossover bits
	public final void crossOver(Chomosone other) {

		// Should we cross over?
		if (Configure.rand.nextDouble() > Configure.crossRate)
			return;

		// Generate a random position
		int pos = Configure.rand.nextInt(chromo.length());

		// Swap all chars after that position
		for (int x = pos; x < chromo.length(); x++) {
			// Get our character
			char tmp = chromo.charAt(x);

			// Swap the chars
			chromo.setCharAt(x, other.chromo.charAt(x));
			other.chromo.setCharAt(x, tmp);
		}
	}

	// Mutation
	public final void mutate() {
		for (int x = 0; x < chromo.length(); x++) {
			if (Configure.rand.nextDouble() <= Configure.mutRate)
				chromo.setCharAt(x, (chromo.charAt(x) == '0' ? '1' : '0'));
		}
	}

	// Add up the contents of the decoded chromo
	public final int addUp() {

		// Decode our chromo
		String decodedString = decodeChromo();

		// Total
		int tot = 0;

		// Find the first number
		int ptr = 0;
		while (ptr < decodedString.length()) {
			char ch = decodedString.charAt(ptr);
			if (Character.isDigit(ch)) {
				tot = ch - '0';
				ptr++;
				break;
			} else {
				ptr++;
			}
		}

		// If no numbers found, return
		if (ptr == decodedString.length())
			return 0;

		// Loop processing the rest
		boolean num = false;
		char oper = ' ';
		while (ptr < decodedString.length()) {
			// Get the character
			char ch = decodedString.charAt(ptr);

			// Is it what we expect, if not - skip
			if (num && !Character.isDigit(ch)) {
				ptr++;
				continue;
			}
			if (!num && Character.isDigit(ch)) {
				ptr++;
				continue;
			}

			// Is it a number
			if (num) {
				switch (oper) {
				case '+': {
					tot += (ch - '0');
					break;
				}
				case '-': {
					tot -= (ch - '0');
					break;
				}
				case '*': {
					tot *= (ch - '0');
					break;
				}
				case '/': {
					if (ch != '0')
						tot /= (ch - '0');
					break;
				}
				}
			} else {
				oper = ch;
			}

			// Go to next character
			ptr++;
			num = !num;
		}

		return tot;
	}

	public final boolean isValid() {

		// Decode our chromo
		String decodedString = decodeChromo();

		boolean num = true;
		for (int x = 0; x < decodedString.length(); x++) {
			char ch = decodedString.charAt(x);

			// Did we follow the num-oper-num-oper-num patter
			if (num == !Character.isDigit(ch))
				return false;

			// Don't allow divide by zero
			if (x > 0 && ch == '0' && decodedString.charAt(x - 1) == '/')
				return false;

			num = !num;
		}

		// Can't end in an operator
		if (!Character.isDigit(decodedString.charAt(decodedString.length() - 1)))
			return false;

		return true;
	}

	@Override
	public int compareTo(Chomosone p1) {
		return new Double(score).compareTo( p1.score);
	}

	
}