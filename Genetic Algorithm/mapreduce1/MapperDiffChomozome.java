package mapreduce1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.commons.math.genetics.Chromosome;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class MapperDiffChomozome extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, OutputCollector output, Reporter reporter) throws IOException {
		// Prepare the input data.
		System.out.println("Am in mapper");
		ArrayList pool = new ArrayList(Configure.poolSize);
		ArrayList newPool = new ArrayList(pool.size());
		for(int i = 0; i < Configure.poolSize; i++){
		pool.add(new Chomosone(Integer.parseInt(value.toString())));
		}
		Text parentOne = new Text("");
	
		 Text parentTwo = new Text(""); 
		Collections.sort(pool);
			for (int i = 0; i < Configure.poolSize; i += 2) {
			parentOne.set(((Chomosone)pool.get(i)).chromo.toString());
			parentTwo.set(((Chomosone)pool.get(i + 1)).chromo.toString());
			output.collect(parentOne, parentTwo);
		}
	}
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
}
