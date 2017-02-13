package speciesViewer;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapred.JobClient; 
import org.apache.hadoop.mapred.JobConf; 

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

public class SpeciesViewerDriver { 
	  
	   public static void main(String[] args) { 
	     JobClient client = new JobClient(); 
	     JobConf conf = new JobConf(SpeciesViewerDriver.class); 
	     conf.setJobName("Species Viewer"); 
	  
	     //~dk
	     //conf.setInputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class); 

	     conf.setOutputKeyClass(FloatWritable.class); 
	     conf.setOutputValueClass(Text.class); 
	  
	     if (args.length < 2) { 
	       System.out.println("Usage: SpeciesViewerDriver <input path> <output path>"); 
	       System.exit(0); 
	     } 

	     //~dk
	     //conf.setInputPath(new Path(args[0])); 
	     //conf.setOutputPath(new Path(args[1])); 
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	  
	     conf.setMapperClass(SpeciesViewerMapper.class); 
	     conf.setReducerClass(org.apache.hadoop.mapred.lib.IdentityReducer.class); 
	  
	     client.setConf(conf); 
	     try { 
	       JobClient.runJob(conf); 
	     } catch (Exception e) { 
	       e.printStackTrace(); 
	     } 
	   } 
	 } 
	 