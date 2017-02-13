package org.abm.mapReduce1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * input key - PAN of the depositter,location value - tranactionId, amount,
 * lastWithdrawer PAN,location
 * 
 * output key-PAN Value- amount,lastWithdrawerPAN,location
 * 
 * @author Aks
 *
 */
public class Mapper3_1 extends Mapper<LongWritable, Text, Text, Text> {
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineArray = line.split("\t");
		String valueString = lineArray[1];
		String[] entry = valueString.split(",");
		ArrayList<String> list = new ArrayList<>(Arrays.asList(entry));
		// String textKey = entry1[0];
		list.remove(0);
		String amount = list.get(0);
		list.remove(0);
		list.add(amount);

		StringBuilder result = new StringBuilder();
		for (String string : list) {
			result.append(string);
			result.append(",");
		}
		String textValue = result.length() > 0 ? result.substring(0, result.length() - 1) : "";
		System.out.println(lineArray[0] + " " + textValue.toString());
		context.write(new Text(lineArray[0]), new Text(textValue));
	}
}