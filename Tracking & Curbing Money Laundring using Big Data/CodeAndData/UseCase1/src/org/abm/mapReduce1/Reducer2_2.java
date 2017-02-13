package org.abm.mapReduce1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * input 
 * For serial Number for which type1Input present(blackListed Note)
 * key-transaction value-
 * serial,Amount,PAN,TimeStamp,Location,Type,blacListingTimeStamp,
 * lastWithdrawer,location
 * 
 * For serial number for which type1input absent(not a blacklitedNote)
 * key-serial value-
 * TransactionID,Amount,PAN,TimeStamp,Location,Type
 * output
 * key - PAN of the depositter
 * value - tranactionId, amount, lastWithdrawer PAN, 
 * @author Aks
 *
 */
public class Reducer2_2 extends Reducer<Text, Text, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		List<String> list = new ArrayList<>();
		int trueCounter = 0;
		for(Text val : values){
			String s = val.toString();
			String[] array = s.split(",");
			if(array.length > 6){
				trueCounter++;
			}
			list.add(s);
		}System.out.println("True Count>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + trueCounter +" list size =" + trueCounter/list.size());
		
		if(((float)trueCounter/list.size()) >= 0.7){
			System.out.println("True Count>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>oyr>>>>>>>>>>>>>>>>>>>" + trueCounter);
			Map<String, String> map = new HashMap<>();
			for(String s : list){
				String[] array = s.split(",");
				if(array.length > 6){
				map.put((array[7]), s);
				}
			}for(String x : map.keySet()){
				String[] val = map.get(x).split(",");
				String value = key.toString() + ","+ val[1] + "," + x + "," + val[8];
				context.write(new Text(val[2]+","+val[4]), new Text(value));
			}
		}
		
	}
}