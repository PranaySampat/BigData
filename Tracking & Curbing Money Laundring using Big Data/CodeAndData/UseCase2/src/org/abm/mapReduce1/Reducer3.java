package org.abm.mapReduce1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * input key-transaction PAIR Value-
 * depAmount,depPAN,depTimeStamp,depLocation,notesCount,wPAN,wLocation output
 * key-PAN value- withdrawer PAN, count
 * 
 * @author Aks
 *
 */
public class Reducer3 extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Iterator<Text> it = values.iterator();
		int count = 0;
		String s = "";
		while (it.hasNext()) {
			count++;
			s = it.next().toString();
		}
		String[] array = s.split(",");
		if (((float) count / Integer.parseInt(array[4])) >= 0.4) {
			String value = array[1] + "," + array[3] + "," + array[5] + "," + array[6] + "," + array[0];
			context.write(null, new Text(value));
		}
	}
}
