package frequentItems;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class freq {
static String num=null;
public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
private Text item = new Text();
int i=0;
 public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
   i++;
     if(i==1)
   {
       num=value.toString();
       System.out.println(num);
   }
   else
   {
     String line = value.toString();
     String parts[]=line.split(",");
   
    List<Integer> list = new ArrayList<Integer>();
     for(int i=1;i<=parts.length-1;i++)
     {
         list.add(Integer.parseInt(parts[i]));
     }
     Collections.sort(list);
     List<String> list1 = new ArrayList<String>();
     for(int i=0;i<=list.size()-1;i++)
     {
    	 list1.add(list.get(i).toString());
     }
    for(int i=0;i<=list.size()-1;i++)
    {
    	//System.out.println(list.get(i));
    //	output.collect(new Text(list.get(i).toString()),new IntWritable(1));
    	
    	for(int j=i+1;j<=list.size()-1;j++)
    	{
    		//System.out.println(list.get(i)+" "+list.get(j));
    	//	output.collect(new Text(list.get(i).toString()+" "+list.get(j).toString()),new IntWritable(1));
    		list1.add((list.get(i)+","+list.get(j)).toString());
    	}
    }
    for(int i=0;i<=list1.size()-1;i++)
    {
    	
    	output.collect(new Text(list1.get(i)),new IntWritable(1));
    }
     
    
     
   }
 }
}
 

public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
 public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
   int sum = 0;
   while (values.hasNext()) {
     sum += values.next().get();
   }
 if(key.charAt(0)=='2'&&key.charAt(1)=='1')
 {
	 System.out.println(key+" "+sum);
 }
   if(sum>=Integer.parseInt(num))
   {
   output.collect(key, new IntWritable(sum));
 }
 }
}

public static void main(String[] args) throws Exception {
    long startTime = System.currentTimeMillis();
    
    File folder= new File(args[0]);
  
            readFile(folder);              
      
    long stopTime = System.currentTimeMillis();
     long elapsedTime = stopTime - startTime;
     System.out.println(elapsedTime);
         }

 private static void readFile(File file) throws IOException {
    String name=file.getAbsolutePath();
    String name2=file.getParent()+"/output";
JobConf conf = new JobConf(freq.class);
 conf.setJobName("wordcount");

 conf.setOutputKeyClass(Text.class);
 conf.setOutputValueClass(IntWritable.class);

 conf.setMapperClass(Map.class);
 conf.setCombinerClass(Reduce.class);
 conf.setReducerClass(Reduce.class);

 conf.setInputFormat(TextInputFormat.class);
 conf.setOutputFormat(TextOutputFormat.class);

 FileInputFormat.setInputPaths(conf, new Path(name));
 FileOutputFormat.setOutputPath(conf, new Path(name2));

 JobClient.runJob(conf);
}
}



