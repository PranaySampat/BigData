package mapreduce1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.math.genetics.Chromosome;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.JobID;

public class ReducerDiffChomozome extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	static int a = 0;

	@Override
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		int target = 42;
		
		String parent1 = key.toString();
		 String parent2 = values.next().toString();
		 Chomosone p1 = new Chomosone(new StringBuffer(parent1));
		 Chomosone p2 = new Chomosone(new StringBuffer(parent2));
		///////////////////////////////
		//int target = Integer.parseInt(key.toString());

		//
				// Create the pool
		ArrayList pool = new ArrayList(Configure.poolSize);
		ArrayList newPool = new ArrayList(pool.size());
		pool.add(p1);
		pool.add(p2);

		int gen = 0;

		//
		int g = 0;
		while (true) {
			g++;
			// Clear the new pool
			newPool.clear();

			// Add to the generations
			gen++;

			// Loop until the pool has been processed
			for (int x = pool.size() - 1; x >= 0; x -= 2) {
				// Select two members
				//Chomosone n = new Chomosone(5);
				Chomosone n1 = selectMember(pool);// selectMember(pool);
				Chomosone n2 = selectMember(pool);

				// Cross over and mutate
				n1.crossOver(n2);
				n1.mutate();
				n2.mutate();
				

				// Rescore the nodes
				n1.scoreChromo(target);
				n2.scoreChromo(target);

				// Check to see if either is the solution
				if (n1.total == target && n1.isValid()) {
					System.out.println("Generations: " + gen + "  Solution: " + n1.decodeChromo() + " - " + g);
					output.collect(key, new Text("Generations: " + gen + "  Solution: " + n1.decodeChromo()));
					return;
				}
				if (n2.total == target && n2.isValid()) {
					System.out.println("Generations: " + gen + "  Solution: " + n2.decodeChromo() + " - " + g);
					output.collect(key, new Text("Generations: " + gen + "  Solution: " + n2.decodeChromo()));
					return;
				}

				// Add to the new pool
				newPool.add(n1);
				newPool.add(n2);
			}

			// Add the newPool back to the old pool
			pool.addAll(newPool);
		}
		//

	}
	//
	public Chomosone selectMember(ArrayList l) {

		// Get the total fitness
		double tot = 0.0;
		for (int x = l.size() - 1; x >= 0; x--) {
			double score = ((Chomosone) l.get(x)).score;
			tot += score;
		}
		double slice = tot * Configure.rand.nextDouble();

		// Loop to find the node
		double ttot = 0.0;
		for (int x = l.size() - 1; x >= 0; x--) {
			Chomosone node = (Chomosone) l.get(x);
			ttot += node.score;
			if (ttot >= slice) {
				l.remove(x);
				return node;
			}
		}

		return (Chomosone) l.remove(l.size() - 1);
	}
	// }
	String jobId; 
	JobConf conf;
	@Override
	public void configure(JobConf conf) { this.conf = conf;
	jobId = conf.get("mapred.job.id"); }
}
