package mapreduce1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class DiffChomozome {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		JobClient jobClientBuilder = new JobClient();
		JobConf configBuilder = new JobConf(DiffChomozome.class);
		configBuilder.setJobName("MAP REDUCE EVOLUTION");
		configBuilder.setMapperClass(MapperDiffChomozome.class);
		configBuilder.setOutputKeyClass(Text.class);
		configBuilder.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(configBuilder, new Path(args[0]));

		configBuilder.setReducerClass(ReducerDiffChomozome.class);

		FileOutputFormat.setOutputPath(configBuilder, new Path(args[1]));
		jobClientBuilder.setConf(configBuilder);
		System.out.println("Driver");
		try {
			JobClient.runJob(configBuilder);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
	}

}
