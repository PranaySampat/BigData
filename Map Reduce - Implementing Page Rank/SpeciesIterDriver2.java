// 
 // Author - Jack Hebert (jhebert@cs.washington.edu) 
 // Copyright 2007 
 // Distributed under GPLv3 
 // 
// Modified - Dino Konstantopoulos
// Distributed under the "If it works, remolded by Dino Konstantopoulos, 
// otherwise no idea who did! And by the way, you're free to do whatever 
// you want to with it" dinolicense
// 
package U.CC;

 import org.apache.hadoop.fs.Path; 
 import org.apache.hadoop.io.IntWritable; 
 import org.apache.hadoop.io.Text; 
 import org.apache.hadoop.mapred.JobClient; 
 import org.apache.hadoop.mapred.JobConf; 
 import org.apache.hadoop.mapred.Mapper; 
 import org.apache.hadoop.mapred.Reducer; 
import org.apache.hadoop.fs.FileSystem;
import java.security.*;
import javax.security.auth.callback.*;
import javax.crypto.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
  
  
 public class SpeciesIterDriver2 { 
  //private static final String OUT_FILE_PRFX = "ITERATION";
 public static void main(String[] args) { 
if (args.length < 2) { 
 System.out.println("Usage: PageRankIter <input path> <output path>"); 
 System.exit(0); 
 } 
 
String input = args[0];
 for(int i=0;i<10;i++){    
 JobClient client = new JobClient(); 
 JobConf conf = new JobConf(SpeciesIterDriver2.class); 
 conf.setJobName("Species Iter" + (i+1)); 
  
 conf.setNumReduceTasks(5); 
  
 //~dk
 //conf.setInputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class); 
 //conf.setOutputFormat(org.apache.hadoop.mapred.SequenceFileOutputFormat.class); 
  
 conf.setOutputKeyClass(Text.class); 
 conf.setOutputValueClass(Text.class); 
  
 conf.setMapperClass(SpeciesIterMapper2.class); 
 conf.setReducerClass(SpeciesIterReducer2.class); 
 conf.setCombinerClass(SpeciesIterReducer2.class); 

 //~dk
 //conf.setInputPath(new Path(args[0])); 
 //conf.setOutputPath(new Path(args[1])); 
 String output = ("Out_Iter" + (i+1));
 Path outputpath = new Path(output);
 FileInputFormat.setInputPaths(conf, new Path(input));
 FileOutputFormat.setOutputPath(conf, outputpath);
  
 //conf.setInputPath(new Path("graph2")); 
 //conf.setOutputPath(new Path("graph3")); 
  
 
  
 client.setConf(conf); 
 try { 
    FileSystem dfs = FileSystem.get(outputpath.toUri(),conf);
    if(dfs.exists(outputpath)){
        dfs.delete(outputpath,true);
    }
 JobClient.runJob(conf); 
 } catch (Exception e) { 
 e.printStackTrace(); 
 }
 input = output;

 }
 } 
 } 
 