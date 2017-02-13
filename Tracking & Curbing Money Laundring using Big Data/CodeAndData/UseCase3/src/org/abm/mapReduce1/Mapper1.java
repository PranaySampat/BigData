package org.abm.mapReduce1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//processes blackListedList
/**
 * Input Format DepPan,DepLocation,WithPan,WithLocation,Amount OutputFormat key
 * - depPAN Value-withPAN
 * 
 * @author Akshay
 *
 */
public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] entry = line.split(",");
		context.write(new Text(entry[0].trim()), new Text(entry[2].trim()));
	}

}