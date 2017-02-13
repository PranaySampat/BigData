package org.abm.mapReduce1;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * input - key - txnID Value-
 * TxnID,Amount,PAN,TimeStamp,Location,Type,currencySerialNumber
 * 
 * output - key - txnID Value-
 * TxnID,Amount,PAN,TimeStamp,Location,Type,currencySerialNumber,notesCount
 * 
 * @author Aks
 *
 */

public class Reducer1 extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// Configuration conf = context.getConfiguration();
		int count = 0;
		ArrayList<String> list = new ArrayList<>();
		while (values.iterator().hasNext()) {
			count++;
			list.add(values.iterator().next().toString());
		}
		for (String x : list) {
			context.write(key, new Text(x + "," + String.valueOf(count)));
		}
	}
}