import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Dali {
        
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
 
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // private MapWritable arry = new MapWritable();
        // if line contain '大里' then keep line else delete;
        String delimiter = ","; 
        String line = value.toString();
        int count =0; //利用 count 控制欄位
        StringTokenizer tokenizer = new StringTokenizer(line,delimiter);
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            if (token.equals("大里")){
               count += 1;
            }
            if (token.equals("PM2.5")){
                count += 1;
            }
            if (count == 2) {
                context.write(new Text("dali"), new Text(line));
                count = 0;

            }
    }

    // public void cleanup(Context context) throws IOException, InterruptedException {
    //    context.write(new Text("dali"), new Text(line));
    // }

 } 
}       
 public static class Reduce extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        for (Text reading : values) {
            context.write(new Text(reading), new Text());
        }
  }
}      
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "dali");
    
    job.setOutputKeyClass(Text.class);
    // job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setJarByClass(WordCount.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }      
 
}

