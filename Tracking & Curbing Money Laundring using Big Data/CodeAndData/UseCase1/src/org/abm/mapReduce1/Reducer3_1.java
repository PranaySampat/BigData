package org.abm.mapReduce1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * input key-PAN Value- amount,lastWithdrawerPAN output key-NULL value-
 * PAN,LOCATION,withdrawer PAN,LOCATION,AMOUNT
 * 
 * @author Aks
 *
 */
public class Reducer3_1 extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for(Text value : values){
		context.write(null, new Text(key.toString() + "," + value));
		}
	}
}
