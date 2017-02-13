package org.abm.mapReduce1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver1 extends Configured implements Tool {
	public int run(String[] args) throws Exception {

		Configuration c = new Configuration();
		Path p1 = new Path(args[0]);
		Path p2 = new Path(args[1]);
		

		Job job = new Job(c, "Stage 1");
		job.setJarByClass(Driver1.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, p1);
		FileOutputFormat.setOutputPath(job, p2);
		job.submit();
		job.waitForCompletion(true);

		
		boolean flag3 = job.waitForCompletion(true);
		return flag3 ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			System.err.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation>");
			System.exit(0);
		}
		ToolRunner.run(new Configuration(), new Driver1(), args);
	}
}
