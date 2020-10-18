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
        
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
 
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // private MapWritable arry = new MapWritable();
        // if line contain '大里' then keep line else delete;
        String delimiter = ","; 
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line,delimiter);
        int count = 0;
        String line1="";
        String prefix = "";
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            count += 1;
            if (count>3) {
                if (token.trim().equals("NR")){
                    token="0";
                }
                String rtoken = "";
                String[] tokens = token.split("");
                    for (String ele:tokens) {
                        if (ele.equals("#")){ ele = "";}
                        if (ele.equals("*")){ ele = "";}
                        if (ele.equals("/")){ ele = "";}
                        if (ele.equals("_")){ ele = "";}
                        if (ele.equals("x")){ ele = "";}

                        rtoken +=ele;

                    }
                token = rtoken;
            } 
            // if (count>3 && token.trim().equals("NR")){token="0";}
            // if (count>3 && toke1.split 如過遇到 # 就刪除掉這個字元, 然後再合併寫回token)
            
            line1 += prefix + token;
            prefix = ",";
        }
        context.write(new Text("whatever"), new Text(line1));
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

