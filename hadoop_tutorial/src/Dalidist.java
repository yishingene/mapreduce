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
        
public class Dalidist {
        
 public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
 
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    private MapWritable arry = new MapWritable();
    String line = value.toString();
    StringTokenizer tokenizer = new StringTokenizer(line,",");
    int position=0;
    int count=0;
    while (tokenizer.hasMoreTokens()) {
        String token = tokenizer.nextToken();
        position ++;
        count++;
        if (count==1){String keydate=token;}
        arry.put(new IntWritable(position),token);

        
    }
    context.write(keydate, arry);
 

 } 
}       
 public static class Reduce extends Reducer<Text, MapWritable, Text, Text> {
    public void reduce(Text key, Iterable<MapWritable> arry, Context context) 
      throws IOException, InterruptedException {
        for (MapWritable ar : arry) {
            context.write(new Text(key), ((Text)ar.get(new IntWritable(0))).get());
        }
  }
}      
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "dali");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapOutputValueClass(MapWritable.class); // array    
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

