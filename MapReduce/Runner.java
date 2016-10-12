package Cloud.ApacheLog;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.WritableComparator;
import java.nio.ByteBuffer;
public class Runner {

  /**
   * author Pranay
   */
  static class MyComparator extends WritableComparator{

    public MyComparator(){
      super(IntWritable.class);
    }
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

      Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
      Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

      return (v1.compareTo(v2)) * -1;
    }

  }
  public static void main(String[] args) throws Exception
  {
        JobConf conf = new JobConf(Runner.class);
        conf.setJobName("ip-count");
        
        conf.setMapperClass(IpMapper.class);
        
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);
        
        conf.setReducerClass(IpReducer.class);
        
        
        // take the input and output from the command line
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path("temp"));

        JobClient.runJob(conf);
        // New job for sorting
        JobConf conf1 = new JobConf(Runner.class);
        conf1.setMapperClass(IpMapper2.class);
        
        conf1.setMapOutputKeyClass(IntWritable.class);
        conf1.setMapOutputValueClass(Text.class);
        conf1.setOutputKeyComparatorClass(MyComparator.class);
        conf1.setReducerClass(IpReducer2.class);
        FileInputFormat.setInputPaths(conf1, new Path("temp"));
        FileOutputFormat.setOutputPath(conf1, new Path(args[1]));
        JobClient.runJob(conf1);
	}

}
