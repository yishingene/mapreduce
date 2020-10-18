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
        
public class Daliword {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
 
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // private MapWritable arry = new MapWritable();
        // if line contain '大里' then keep line else delete;
        String line = value.toString();
        String line2 = line.replaceAll("", " ");
        StringTokenizer tokenizer = new StringTokenizer(line2," ");
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            context.write(new Text(token), new IntWritable(1));
            
    }

    // public void cleanup(Context context) throws IOException, InterruptedException {
    //    context.write(new Text("dali"), new Text(line));
    // }

 } 
}       
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
  }
}      
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "dali");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
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

