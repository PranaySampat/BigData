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
 * Input Key : depPAN Value: withPAN output key :null value :
 * withPAN1,withPAN2,......
 * 
 * @author Aks
 *
 */

public class Reducer1 extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String s = ":";
		for (Text val : values) {
			s = s + " " +val.toString();
		}
		context.write(key, new Text(s));

	}

}