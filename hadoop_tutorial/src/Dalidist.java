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
    private MapWritable arry = new MapWritable();
    private Text keydate = new Text();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    
    String line = value.toString();
    StringTokenizer tokenizer = new StringTokenizer(line,",");
    int position=0;
    int count=0;
    while (tokenizer.hasMoreTokens()) {
        String token = tokenizer.nextToken();
        position ++;
        count++;
        if (count==1){String keydate=token;}
        double tmpparse = Double.parseDouble(token);
        arry.put(new IntWritable(position),new DoubleWritable(tmpparse));

        
    }
    context.write(keydate, arry);
 

 } 
}       
 public static class Reduce extends Reducer<Text, MapWritable, Text, DoubleWritable> {
    public void reduce(Text key, Iterable<MapWritable> arry, Context context) 
      throws IOException, InterruptedException {
        for (MapWritable ar : arry) {
            double tmp = ((DoubleWritable)ar.get(new IntWritable(0))).get();
            context.write(new Text(key), new DoubleWritable(tmp));
        }
  }
}      
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "dali");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapOutputValueClass(MapWritable.class); // array 
    job.setOutputValueClass(DoubleWritable.class);   
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

