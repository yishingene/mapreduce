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

        
public class Dalikmean {
        
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    // private Text center1 = "new Text()";
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    
      String line = value.toString();
      List<String> list=Arrays.asList(line.split(",")); //將每一列轉為 list
      //計算每一點與四個參考點的距離


        float d1 = 0;
        for(int j = 1; j <= 24; j++) {
          //與第一個參考點距離
          float delta = Float.parseFloat(list.get(1+j))-Float.parseFloat(list.get(25+j));
          float e= (float)Math.pow(delta, 2);
          d1 = d1+e;
        }

        float d2 = 0;
        for(int j = 1; j <= 24; j++) {
          //與第二個參考點距離
          float delta = Float.parseFloat(list.get(1+j))-Float.parseFloat(list.get(49+j));
          float e= (float)Math.pow(delta, 2);
          d2 = d2+e;
        }

        float d3 = 0;
        for(int j = 1; j <= 24; j++) {
          //與第3個參考點距離
          float delta = Float.parseFloat(list.get(1+j))-Float.parseFloat(list.get(73+j));
          float e= (float)Math.pow(delta, 2);
          d3 = d3+e;
        }

        float d4 = 0;
        for(int j = 1; j <= 24; j++) {
          //與第4個參考點距離
          float delta = Float.parseFloat(list.get(1+j))-Float.parseFloat(list.get(97+j));
          float e= (float)Math.pow(delta, 2);
          d4 = d4+e;
        }
        //比較大小,找出最小,寫出 key
        float index=1;
        float small = d1;
    
        if (d2<small){
          small=d2;
          index=2;
        }
        if (d3<small){
          small=d3;
          index=3;
        }
        if (d4<small){
          small=d4;
          index=4;
        }

        //重新組合一個新的 output value key + 第二個欄位以後的資料
        String outputvalue="";
        outputvalue=outputvalue+Float.toString(index);
        for(int j = 1; j <= 121; j++){
          outputvalue=outputvalue+list.get(j);
        }
        String newkey = Float.toString(index);
        context.write(new Text(newkey), new Text(outputvalue));
        
        


        




      // StringTokenizer tokenizer = new StringTokenizer(line,",");
      // int position=0;
      // int count=0;
      
      // while (tokenizer.hasMoreTokens()) {
      //     // String token = tokenizer.nextToken();
      //     position ++;
      //     count ++;
      //     // if (count==1){String keydate=token;}
      //     Double tmpparse = Double.parseDouble(tokenizer.nextToken());
      //     if (count==4){
      //       arry.put(new IntWritable(0),new DoubleWritable(tmpparse));
      //       context.write(word, arry);
      //     }   
      // }
 } 
}       
 public static class Reduce extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        for (Text reading : values) {
          context.write(new Text(key), new Text(reading));
      }

        
  }
}      
 
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "dalikmean");
    
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);  
    job.setOutputKeyClass(Text.class); 
    job.setOutputValueClass(Text.class); 
    job.setJarByClass(Dalikmean.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);


    // job.setOutputKeyClass(Text.class);
    // job.setOutputValueClass(IntWritable.class);
    // job.setMapOutputValueClass(MapWritable.class); // array 
    // job.setOutputValueClass(DoubleWritable.class); 
    // job.setOutputValueClass(FloatWritable.class);    
    // job.setMapperClass(Map.class);
    // job.setReducerClass(Reduce.class);
    // job.setJarByClass(Dalidist.class);
        
    // job.setInputFormatClass(TextInputFormat.class);
    // job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }      
 
}

