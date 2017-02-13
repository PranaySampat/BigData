package org.abm.mapReduce1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Input key - serial Number
 * Value-TransactionID,Amount,PAN,TimeStamp,Location,Type,blacListingTimeStamp,
 * lastWithdrawer,location
 * 
 * Output key -TransactionId 
 * Value - serialNumber,Amount,PAN,TimeStamp,Location,Type,blacListingTimeStamp,
 * lastWithdrawer,lastWithdrawalTIme,location
 * (last three fields may or may not be present
 * depending upon whether blackListed note or not)
 * 
 * @author Akshay
 *
 */
public class Mapper2_1 extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineArray = line.split("\t");
		String serialNumber = lineArray[0];
		String valueString = lineArray[1];
		String[] entry  = valueString.split(",");
		ArrayList<String> list = new ArrayList<>(Arrays.asList(entry));
		String txId = list.get(0);
		list.set(0, serialNumber);
		StringBuilder result = new StringBuilder();
		for (String string : list) {
			result.append(string);
			result.append(",");
		}
		String textValue = result.length() > 0 ? result.substring(0, result.length() - 1) : "";
		//Configuration conf = context.getConfiguration();

		System.out.println(txId +" jjjjjjjjjj" + textValue.toString());
		context.write(new Text(txId), new Text(textValue));
		
	}
}