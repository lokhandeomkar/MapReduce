//    import java.io.IOException;
//    import java.text.ParseException;
//    import java.text.SimpleDateFormat;
//    import java.util.*;
//
//    import org.apache.hadoop.fs.Path;
//    import org.apache.hadoop.io.*;
//    import org.apache.hadoop.mapreduce.*;
//    import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//    import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//    import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//    import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//    
//    public class MP12 {
//    	
//     public static class Map extends Mapper<LongWritable, Text, Text, Text> {
//        private Text word = new Text();
//        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            String line = value.toString();
//            List<String> items = new ArrayList<String>();
//            String[] tk = line.split(",",-1);
//            for(int i = 0; i < tk.length; i++)
//            {
//                if(tk[i].isEmpty())
//                {
//                    tk[i] = "null";
//                }
//            }
//            int i = 0;
//            while (i < tk.length) {
//                i = i+1;
//                if (i == 1 || i == 2 || i == 4){
//                    items.add(tk[i-1]);
//                }
//            }
//            // now items is ymdh, uid, click
//            if (Long.parseLong(items.get(2).trim())==1 && items.get(1) != "null"){
//            	SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            	try {
//            		Date date = formatter.parse(items.get(0));
//            		//Calendar calendar = Calendar.getInstance();
//            		//calendar.setTime(date);
//            		long mseconds = date.getTime(); //calendar.get(Calendar.SECOND);
//            		word.set(Long.toString(mseconds));       
//                	context.write(word, new Text(items.get(0)+","+items.get(1)));
//                	word.set(Long.toString(mseconds-1000));       
//                	context.write(word, new Text(items.get(0)+","+items.get(1)));
//            	} catch (ParseException e) {
//            		e.printStackTrace();
//            	}
//            }
//         }
//     }
//
//    public static class Reduce extends Reducer<Text, Text, Text, Text> {
//
//        public void reduce(Text key, Iterable<Text> values, Context context) 
//          throws IOException, InterruptedException {
//        	HashMap<Long, String> mp = new HashMap<Long, String>();
//            for (Text val : values) {
//            	//context.write(val,val);
//                String[] strng = (val.toString()).split(","); // ymdh and uid
//                mp.put(Long.parseLong(strng[1].trim()), strng[0]); // k,v = uid,ymdh 
//            }
//            Set<Long> keys = mp.keySet();
//            Object[] karr = keys.toArray();
//            Arrays.sort(karr);
//            for (int i = 0; i < karr.length-1; i++) {       
//            	context.write(new Text(mp.get(karr[i]) + ", " + Long.toString((long) karr[i]) + ", " + Long.toString((long) karr[i+1])), new Text("")); // yhdh1, uid1, uid2
//            }
//        }
//     }
//
//     public static void main(String[] args) throws Exception {
//            	
//        Job job = Job.getInstance();
//        job.setJarByClass(MP12.class);
//        
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        
//        job.setMapperClass(Map.class);
//        job.setReducerClass(Reduce.class);
//        
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//        
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        
//        job.waitForCompletion(true);
//     }
//     
// }
//    


/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MP12 {  //extends Configured implements Tool {

 private static final String OUTPUT_PATH = "intermediate_output";
 
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
     private Text word = new Text();
     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String line = value.toString();
         List<String> items = new ArrayList<String>();
         String[] tk = line.split(",",-1);
         for(int i = 0; i < tk.length; i++)
         {
             if(tk[i].isEmpty())
             {
                 tk[i] = "null";
             }
         }
         int i = 0;
         while (i < tk.length) {
             i = i+1;
             if (i == 1 || i == 2 || i == 4){
                 items.add(tk[i-1]);
             }
         }
         // now items is ymdh, uid, click
         if (Long.parseLong(items.get(2).trim())==1 && items.get(1) != "null"){
         	SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
         	try {
         		Date date = formatter.parse(items.get(0));
         		//Calendar calendar = Calendar.getInstance();
         		//calendar.setTime(date);
         		long mseconds = date.getTime(); //calendar.get(Calendar.SECOND);
         		word.set(Long.toString(mseconds));       
            context.write(word, new Text(items.get(0)+","+items.get(1)));
            word.set(Long.toString(mseconds-1000));       
            context.write(word, new Text(items.get(0)+","+items.get(1)));
         	} catch (ParseException e) {
         		e.printStackTrace();
         	}
         }
      }
  }

 public static class Reduce extends Reducer<Text, Text, Text, Text> {

     public void reduce(Text key, Iterable<Text> values, Context context) 
       throws IOException, InterruptedException {
     	HashMap<Long, String> mp = new HashMap<Long, String>();
         for (Text val : values) {
         	//context.write(val,val);
             String[] strng = (val.toString()).split(","); // ymdh and uid
             mp.put(Long.parseLong(strng[1].trim()), strng[0]); // k,v = uid,ymdh 
         }
         Set<Long> keys = mp.keySet();
         Object[] karr = keys.toArray();
         Arrays.sort(karr);
         for (int i = 0; i < karr.length-1; i++) {
              for (int j = i+1; j < karr.length; j++) {       
         	        context.write(new Text(mp.get(karr[i]) + ", " + Long.toString((long) karr[i]) + ", " + Long.toString((long) karr[j])), new Text(""));
         }
     }
  }
}  

  public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
     private Text word = new Text();
     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String line = value.toString();
         word.set(line);
         context.write(word, new Text("whatever"));
         }
     }
  
  public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
	     public void reduce(Text key, Iterable<Text> values, Context context) 
	       throws IOException, InterruptedException {   
	         	context.write(key,new Text(""));
	         }
	  }
  
// @Override
// public int run(String[] args) throws Exception {
//  /*
//   * Job 1
//   */
//	 
//	 Job job = Job.getInstance();
//     job.setJarByClass(MP12.class);
//     
//     job.setOutputKeyClass(Text.class);
//     job.setOutputValueClass(Text.class);
//     
//     job.setMapperClass(Map.class);
//     job.setReducerClass(Reduce.class);
//     
//     job.setInputFormatClass(TextInputFormat.class);
//     job.setOutputFormatClass(TextOutputFormat.class);
//     
//     FileInputFormat.addInputPath(job, new Path(args[0]));
//     FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
//     
//     job.waitForCompletion(true);
//	 
//	 ///////////
////  Configuration conf = getConf();
////  FileSystem fs = FileSystem.get(conf);
////  Job job = new Job(conf, "Job1");
////  job.setJarByClass(ChainJobs.class);
////
////  job.setMapperClass(MyMapper1.class);
////  job.setReducerClass(MyReducer1.class);
////
////  job.setOutputKeyClass(Text.class);
////  job.setOutputValueClass(IntWritable.class);
////
////  job.setInputFormatClass(TextInputFormat.class);
////  job.setOutputFormatClass(TextOutputFormat.class);
////
////  TextInputFormat.addInputPath(job, new Path(args[0]));
////  TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
////
////  job.waitForCompletion(true);
//
//  /*
//   * Job 2
//   */
//     
//     Job job2 = Job.getInstance();
//     job2.setJarByClass(MP12.class);
//     
//     job2.setOutputKeyClass(Text.class);
//     job2.setOutputValueClass(Text.class);
//     
//     job2.setMapperClass(Map2.class);
//     job2.setReducerClass(Reduce2.class);
//     
//     job.setInputFormatClass(TextInputFormat.class);
//     job.setOutputFormatClass(TextOutputFormat.class);
//     
//     FileInputFormat.addInputPath(job, new Path(OUTPUT_PATH));
//     FileOutputFormat.setOutputPath(job, new Path(args[1])); 
// 
//  return job2.waitForCompletion(true) ? 0 : 1;
// }

 /**
  * Method Name: main Return type: none Purpose:Read the arguments from
  * command line and run the Job till completion
 * @return 
  * 
  */
 public static void main(String[] args) throws Exception {
  // TODO Auto-generated method stub
  if (args.length != 2) {
   System.err.println("Enter valid number of arguments <Inputdirectory>  <Outputlocation>");
   System.exit(0);
  }
  Job job = Job.getInstance();
  job.setJarByClass(MP12.class);
  
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(Text.class);
  
  job.setMapperClass(Map.class);
  job.setReducerClass(Reduce.class);
  
  job.setInputFormatClass(TextInputFormat.class);
  job.setOutputFormatClass(TextOutputFormat.class);
  
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
  
  job.waitForCompletion(true);
	 
	 ///////////
//Configuration conf = getConf();
//FileSystem fs = FileSystem.get(conf);
//Job job = new Job(conf, "Job1");
//job.setJarByClass(ChainJobs.class);
//
//job.setMapperClass(MyMapper1.class);
//job.setReducerClass(MyReducer1.class);
//
//job.setOutputKeyClass(Text.class);
//job.setOutputValueClass(IntWritable.class);
//
//job.setInputFormatClass(TextInputFormat.class);
//job.setOutputFormatClass(TextOutputFormat.class);
//
//TextInputFormat.addInputPath(job, new Path(args[0]));
//TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
//
//job.waitForCompletion(true);

/*
* Job 2
*/
  
  Job job2 = Job.getInstance();
  job2.setJarByClass(MP12.class);
  
  job2.setOutputKeyClass(Text.class);
  job2.setOutputValueClass(Text.class);
  
  job2.setMapperClass(Map2.class);
  job2.setReducerClass(Reduce2.class);
  
  job2.setInputFormatClass(TextInputFormat.class);
  job2.setOutputFormatClass(TextOutputFormat.class);
  
  FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
  FileOutputFormat.setOutputPath(job2, new Path(args[1])); 

  job2.waitForCompletion(true);
  //return job2.waitForCompletion(true) ? 0 : 1;

  //ToolRunner.run(new Configuration(), new MP12(), args);
 }
 }
