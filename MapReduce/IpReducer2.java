package Cloud.ApacheLog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * Counts all of the hits for an ip. Outputs all ip's
 */
public class IpReducer2 extends MapReduceBase implements Reducer<IntWritable, Text, Text, IntWritable> 
{

  public void reduce(IntWritable key, Iterator<Text> ips,
      OutputCollector<Text, IntWritable> processedOutput, Reporter reporter)
      throws IOException {
    Text ipAdd = new Text();
    while(ips.hasNext()){
      ipAdd = ips.next();
    }
    processedOutput.collect(ipAdd,key);
    
  }

}
