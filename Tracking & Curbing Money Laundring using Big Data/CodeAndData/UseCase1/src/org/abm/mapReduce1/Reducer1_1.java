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
 * Reducer of stage1 in useCase1 2 types of inputs- type 1 input - 
 * key -
 * serialNumber Value- blacListingTimeStamp,lastWithdrawer
 * type 2 input - key - serialNumber 
 * Value-
 * TransactionID,Amount,PAN,TimeStamp,Location,Type
 * 
 * output - For serial Number for which type1Input present(blackListed Note)
 * key-null 
 * value-
 * serial,TransactionID,Amount,PAN,TimeStamp,Location,Type,blacListingTimeStamp,
 * lastWithdrawer
 * 
 * For serial number for which type1input absent(not a blacklitedNote)
 * key-null value- serial,TransactionID,Amount,PAN,TimeStamp,Location,Type
 * 
 * @author Aks
 *
 */

public class Reducer1_1 extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// Configuration conf = context.getConfiguration();
		ArrayList<String> list = new ArrayList<>();
		String blackListString = "";
		for (Text val : values) {
			String[] valueArray = val.toString().split(",");
			// if length is greater than 3 , value must be from mapper2
			if (valueArray.length > 4) {
				list.add(val.toString());
			} else {
				blackListString = val.toString();
			}
		}
		DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy hh:mm a");
		ArrayList<String> blackListedList = new ArrayList<>();

		if (null != blackListString && !blackListString.isEmpty()) {
			for (String s : list) {
				// only consider those entries which are after blacklIsting
				// timestamp
				Date x1;
				Date x2;
				try {
					x1 = dateFormat.parse((s.split(","))[3]);
					x2 = dateFormat.parse((blackListString.split(","))[0]);
				} catch (ParseException e) {
					throw new IllegalArgumentException(e);
				}
				if (null != x2 && null != x1 && x1.after(x2)) {
					blackListedList.add(s);
				}
			}
			Collections.sort(blackListedList, new Comparator<String>() {
				@Override
				public int compare(String o1, String o2) {
					try {
						String[] v1 = o1.split(",");
						String[] v2 = o2.split(",");
						return dateFormat.parse(v1[3]).compareTo(dateFormat.parse(v2[3]));
					} catch (ParseException e) {
						throw new IllegalArgumentException(e);
					}
				}
			});
			if (!blackListedList.isEmpty()) {
				String str = blackListedList.get(0);

				String[] entry = str.split(",");
				int amount = Integer.parseInt(entry[1].trim());
				String type = entry[5].trim();
				if (amount > 500000 && type.equals("D")) {
					str = str + "," + blackListString;
					context.write(key, new Text(str));
				}
			}
		} else {

			for (String s : list) {
				String[] entry = s.split(",");
				int amount = Integer.parseInt(entry[1].trim());
				String type = entry[5].trim();
				if (amount > 500000 && type.equals("D")) {
					context.write(key, new Text(s));
				}
			}
		}

	}

}