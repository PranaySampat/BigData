package org.abm.mapReduce1;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * input Output key -serialNumber value -
 * TxnID,Amount,PAN,TimeStamp,Location,Type,notesCount
 * 
 * output key- transaction PAIRS value -
 * depAmount,depPAN,depTimeStamp,depLocation,notesCount,wPAN,wLocation
 * 
 * @author Aks
 *
 */
public class Reducer2 extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		List<String> list = new ArrayList<>();
		for (Text val : values) {

			list.add(val.toString());
		}
		DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy hh:mm a");
		Collections.sort(list, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				try {
					String[] v1 = o1.split(",");
					String[] v2 = o2.split(",");
					return dateFormat.parse(v2[3]).compareTo(dateFormat.parse(v1[3]));
				} catch (ParseException e) {
					throw new IllegalArgumentException(e);
				}
			}
		});
		String x = list.get(0);
		String[] arrayX = x.split(",");

		if (arrayX[4].equalsIgnoreCase("W")) {
			list.remove(0);
		}
		for (int i = 0; i < list.size() - 1; i = i + 2) {

			String s = list.get(i);
			String s1 = list.get(i + 1);
			String[] array = s.split(",");
			String[] array1 = s1.split(",");
			if (!array[2].equals(array1[2])) {
				String keyString = array[0] + ":" + array1[0];
				StringBuilder result = new StringBuilder();
				ArrayList<String> string1List = new ArrayList<>(Arrays.asList(array));
				string1List.remove(0);
				string1List.remove(4);
				for (String string : string1List) {
					result.append(string);
					result.append(",");
				}
				String textValue = result.length() > 0 ? result.substring(0, result.length() - 1) : "";
				String valString = textValue + "," + array1[2] + "," + array1[4];
				context.write(new Text(keyString), new Text(valString));
			}
		}
	}
}