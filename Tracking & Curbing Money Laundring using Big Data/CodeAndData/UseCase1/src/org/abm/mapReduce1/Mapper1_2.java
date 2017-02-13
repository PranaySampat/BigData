package org.abm.mapReduce1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Input Format
 * TransactionID,Amount,PAN,TimeStamp,Location,Type,currencySerialNumber
 * OutputFormat key - serialNumber Value-
 * TransactionID,Amount,PAN,TimeStamp,Location,Type
 * 
 * @author Akshay
 *
 */
public class Mapper1_2 extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] entry = line.split(",");
		String textKey = entry[6].trim();
		ArrayList<String> list = new ArrayList<>(Arrays.asList(entry));
		list.remove(6);
		StringBuilder result = new StringBuilder();
		for (String string : list) {
			result.append(string);
			result.append(",");
		}
		String textValue = result.length() > 0 ? result.substring(0, result.length() - 1) : "";
		//Configuration conf = context.getConfiguration();

		System.out.println(textKey +"xxxxxx"+ textValue);
		context.write(new Text(textKey), new Text(textValue));
	}
}