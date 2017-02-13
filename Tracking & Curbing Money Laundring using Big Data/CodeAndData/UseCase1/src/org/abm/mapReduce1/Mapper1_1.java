package org.abm.mapReduce1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//processes blackListedList
/**
 * Input Format SerialNumber,blacListingTimeStamp,lastWithdrawer,location
 *  OutputFormat
 * key - serialNumber Value-
 * blacListingTimeStamp,lastWithdrawer,lastWithdrawalTIme,
 * 
 * @author Akshay
 *
 */
public class Mapper1_1 extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] entry = line.split(",");
		String textKey = entry[0].trim();
		StringBuilder result = new StringBuilder();
		for (int i = 1; i < entry.length; i++) {
			result.append(entry[i]);
			result.append(",");
		}
		String textValue = result.length() > 0 ? result.substring(0, result.length() - 1) : "";
		// Configuration conf = context.getConfiguration();

		System.out.println(textKey +"bbbbbbb"+ textValue.toString());
		context.write(new Text(textKey), new Text(textValue));
	}

}