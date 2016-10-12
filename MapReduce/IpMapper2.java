package Cloud.ApacheLog;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Mapper that takes a line from an Apache access log and emits the IP with a
 * count of 1. This can be used to count the number of times that a host has
 * hit a website.
 */
public class IpMapper2 extends MapReduceBase 
                        implements Mapper<LongWritable, Text, IntWritable, Text>
{

  // Regular expression to match the IP at the beginning of the line in an
  // Apache access log
  // private static final Pattern ipPattern = Pattern.compile("^([\\d\\.]+)\\s");

  // Reusable IntWritable for the count
  //private static final IntWritable one = new IntWritable(1);
  
  public void map(LongWritable key, Text value,
      OutputCollector<IntWritable, Text> output, Reporter reporter)
      throws IOException {
      String inputText = value.toString();
      String [] data = inputText.split("\n");
      int count = 0;
      for(String splitData : data){
        String [] lineData = splitData.split("\t");
        count = Integer.parseInt(lineData[1]);
        Text ipAddres = new Text(lineData[0]);
        output.collect(new IntWritable(count),ipAddres);
      }
    
      
    }
  

}
