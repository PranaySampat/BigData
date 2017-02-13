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
		Path p3 = new Path(args[2]);
		Path p4 = new Path(args[3]);

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

		Job job2 = new Job(new Configuration(), "Stage 2");
		job2.setJarByClass(Driver1.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setReducerClass(Reducer2.class);
		job2.setMapperClass(Mapper2.class);

		// take the input and output from the command line
		FileInputFormat.setInputPaths(job2, p2);
		FileOutputFormat.setOutputPath(job2, p3);
		job2.submit();
		job2.waitForCompletion(true);

		Job job3 = new Job(new Configuration(), "Stage 3");
		job3.setJarByClass(Driver1.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		job3.setReducerClass(Reducer3.class);
		job3.setMapperClass(Mapper3.class);

		// take the input and output from the command line
		FileInputFormat.setInputPaths(job3, p3);
		FileOutputFormat.setOutputPath(job3, p4);
		job3.submit();
		boolean flag3 = job3.waitForCompletion(true);
		return flag3 ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 4) {
			System.err.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation>");
			System.exit(0);
		}
		ToolRunner.run(new Configuration(), new Driver1(), args);
	}
}
