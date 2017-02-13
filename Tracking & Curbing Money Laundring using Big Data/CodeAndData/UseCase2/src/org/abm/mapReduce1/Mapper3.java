package org.abm.mapReduce1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * input key- transaction PAIRS value -
 * depAmount,depPAN,depTimeStamp,depLocation,notesCount,wPAN,wLocation
 * 
 * output key-transaction PAIR Value- depAmount,depPAN,depTimeStamp,depLocation,notesCount,wPAN,wLocation
 * 
 * @author Aks
 *
 */
public class Mapper3 extends Mapper<LongWritable, Text, Text, Text> {
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineArray = line.split("\t");
		context.write(new Text(lineArray[0]), new Text(lineArray[1]));
	}
}