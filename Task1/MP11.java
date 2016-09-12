    package org.myorg;

    import java.io.IOException;
    import java.util.*;


    import org.apache.hadoop.fs.Path;
    import org.apache.hadoop.io.*;
    import org.apache.hadoop.mapreduce.*;
    import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
    import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
    import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
    import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

    public class MP11 {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            List<String> items = new ArrayList<String>();
            //StringTokenizer tokenizer = new StringTokenizer(line,",");
            String[] tk = line.split(",");
            int i = 0;
            //while (tokenizer.hasMoreTokens()) {
            while (i < tk.length) {
                i = i+1;
                if (i >= 3 && i <= 6){
                    items.add(tk[i-1]);
                }
            }
            word.set(items.get(3));
            items.remove(3);
            String str = String.join(",", items);       
            context.write(word, new Text(str));
            }
        }

            
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        
        public void reduce(Text key, Iterable<Text> values, Context context) 
          throws IOException, InterruptedException {
            int sumi = 0;
            int sumcli = 0;
            int sumcon = 0;
            
            for (Text val : values) {
                String[] strng = (val.toString()).split(",");
                sumi   += Integer.parseInt(strng[0]);
                sumcli += Integer.parseInt(strng[1]);
                sumcon += Integer.parseInt(strng[2]);
            }
            
            List<String> temp = new ArrayList<String>();
            temp.add(Integer.toString(sumi));
            temp.add(Integer.toString(sumcli));
            temp.add(Integer.toString(sumcon));
            String str = String.join(", ", temp);       
            context.write( new Text(key.toString()+", "+str), new Text(""));
        }
     }
     
    public static void main(String[] args) throws Exception {
        //Configuration conf = new Configuration();

            //Job job = new Job(conf, "MP11");
            Job job = Job.getInstance();
            job.setJarByClass(MP11.class);
            
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
     }

    }