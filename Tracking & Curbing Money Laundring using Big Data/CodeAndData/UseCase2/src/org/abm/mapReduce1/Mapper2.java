package org.abm.mapReduce1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Input key - txnID Value-
 * TxnID,Amount,PAN,TimeStamp,Location,Type,currencySerialNumber,notesCount
 * 
 * Output key -serialNumber value -
 * TxnID,Amount,PAN,TimeStamp,Location,Type,notesCount
 * 
 * @author Akshay
 *
 */
public class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineArray = line.split("\t");
		String valueString = lineArray[1];
		String[] entry  = valueString.split(",");
		ArrayList<String> list = new ArrayList<>(Arrays.asList(entry));
		String serialNumber = list.get(6);
		int amount = Integer.parseInt(list.get(1));
		if (amount >= 500000) {
			StringBuilder result = new StringBuilder();
			list.remove(6);
			for (String string : list) {
				result.append(string);
				result.append(",");
			}
			String textValue = result.length() > 0 ? result.substring(0, result.length() - 1) : "";
			// Configuration conf = context.getConfiguration();

			System.out.println(serialNumber + " XXXX" + textValue.toString());
			context.write(new Text(serialNumber), new Text(textValue));
		}
	}
}