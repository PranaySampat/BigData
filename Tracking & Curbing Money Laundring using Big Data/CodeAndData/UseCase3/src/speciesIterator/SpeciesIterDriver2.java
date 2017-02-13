package speciesIterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

public class SpeciesIterDriver2 {

	
//		if (args.length < 2) {
//			System.out.println("Usage: PageRankIter <input path> <output path>");
//			System.exit(0);
//		}
		 public static void main(String[] args) { 
				String input = args[0];
						
						int noofIterations = Integer.parseInt(args[2]);
						
						for (int i = 0; i < noofIterations; i++) {
				 JobClient client = new JobClient(); 
				 JobConf conf = new JobConf(SpeciesIterDriver2.class); 
				 conf.setJobName("Species Iter_"+(i+1)); 
				  
				 conf.setNumReduceTasks(1); 
				  
				 //~dk
				 //conf.setInputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class); 
				 //conf.setOutputFormat(org.apache.hadoop.mapred.SequenceFileOutputFormat.class); 
				  
				 conf.setOutputKeyClass(Text.class); 
				 conf.setOutputValueClass(Text.class); 
				 conf.set("noOfLinks", args[3]);
				  
				 if (args.length < 2) { 
				 System.out.println("Usage: PageRankIter <input path> <output path>"); 
				 System.exit(0); 
				 } 

				 //~dk
				 //conf.setInputPath(new Path(args[0])); 
				 //conf.setOutputPath(new Path(args[1])); 
				String output = (args[1]+(i+1));
							
							Path outputPath = new Path(output);
				 FileInputFormat.setInputPaths(conf, new Path(input));
				 FileOutputFormat.setOutputPath(conf, outputPath);
				  
				 //conf.setInputPath(new Path("graph2")); 
				 //conf.setOutputPath(new Path("graph3")); 
				  
				 conf.setMapperClass(SpeciesIterMapper2.class); 
				 conf.setReducerClass(SpeciesIterReducer2.class); 
				 conf.setCombinerClass(SpeciesIterReducer2.class); 
				  
				 client.setConf(conf); 
				 try { 
				 FileSystem dfs = FileSystem.get(outputPath.toUri(), conf);
								if (dfs.exists(outputPath)) {
									dfs.delete(outputPath, true);
								}
				 JobClient.runJob(conf); 
				 } catch (Exception e) { 
				 e.printStackTrace(); 
				 } 
				input = output;
				 } 
				 } 
}
