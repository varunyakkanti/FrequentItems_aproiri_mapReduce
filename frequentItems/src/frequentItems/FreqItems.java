package frequentItems;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;

public class FreqItems {
	static HashMap<String, Integer> freq=new HashMap<>();
static String num=null;
static List<String[]> list2 = new ArrayList<>();
static List<String[]> list3 = new ArrayList<>() ;

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
     for(int i=1;i<parts.length;i++)
     {
         list.add(Integer.parseInt(parts[i]));
     }
     Collections.sort(list);
     List<String> list1 = new ArrayList<String>();
    
   for(int i=0;i<list.size();i++)
    {
	   list1.add(list.get(i).toString());
    	
    	for(int j=i+1;j<list.size();j++)
    	{	
    		list1.add((list.get(i)+","+list.get(j)).toString());
    		
    	}
    }

    for(int i=0;i<list1.size();i++)
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

   
   output.collect(key, new IntWritable(sum));

 }
}




//
//
//
//
public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
private Text item = new Text();

 public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
 
   
     String line = value.toString();
     String parts[]=line.split(",");
     ArrayList<String> last= new ArrayList<String>();
     ArrayList<String> allWords = new ArrayList<String>();
     String[] yourArray = Arrays.copyOfRange(parts, 1, parts.length);
     allWords.addAll(Arrays.asList(yourArray));
    
     for(int i=0;i<list2.size();i++)
     {
    	 String[] can=list2.get(i);
    	 ArrayList<Integer> al3= new ArrayList<Integer>();
         for (String temp : can){
            al3.add(allWords.contains(temp) ? 1 : 0);
         }
         int n=0;
         int x=can.length;
         for (int tmp : al3){
        	 if(tmp==1){n=n+1;}
        	 
         }
         if(n==x)
         {String z="";
        	 for (String temp : can){
        		 z=z+temp+",";
        	 }
        	 
        	 output.collect(new Text(z),new IntWritable(1));
         }
         
     }
    
   }
 }

 

public static class Reduce1 extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	   int i=0;
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
   int sum = 0;
i++;
   while (values.hasNext()) {
	   
     sum += values.next().get();
   }
   
   /*System.out.println(key+" "+sum);
    
if(sum>=Integer.parseInt(num)){*/
   if(freq.containsKey(key))
   {freq.put(key.toString(), freq.get(key)+sum);
   	}
   else
   	freq.put(key.toString(), sum);
   
   output.collect(key, new IntWritable(sum));
   
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

 private static <T> void readFile(File file) throws IOException {
    String name=file.getAbsolutePath();
    String name2=file.getParent()+"/Intermediate";
    String name3=file.getParent()+"/output";
JobConf conf = new JobConf(FreqItems.class);
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
 FileReader fileReader = 
         new FileReader(name2+"/part-00000");
 BufferedReader bufferedReader = 
         new BufferedReader(fileReader);
 String line=null;
 List<String[]> al = new ArrayList<>();
 while((line=bufferedReader.readLine())!=null){
	 String[] parts=line.split("\\t");
		 if(Integer.parseInt(parts[1])>=Integer.parseInt(num)){
		 list2.add(parts[0].split(","));
		 if(parts[0].contains(",")){
			 al.add(parts[0].split(","));
		 }
	 }
 }
 
 System.out.println("size"+list2.size());
  generation(al);
 String[]d= list2.get(list2.size()-1);
 for(int j=0;j<d.length;j++){
	 System.out.print(d[j]+",");
 }
 System.out.println("size"+list2.size());

 JobConf conf1 = new JobConf(FreqItems.class);
 conf1.setJobName("wordcount1");

 conf1.setOutputKeyClass(Text.class);
 conf1.setOutputValueClass(IntWritable.class);

 conf1.setMapperClass(Map1.class);
 conf1.setCombinerClass(Reduce1.class);
 conf1.setReducerClass(Reduce1.class);
 /*conf1.setNumMapTasks(5);
 conf1.setNumTasksToExecutePerJvm(5);
conf1.setNumReduceTasks(5);*/
 conf1.setInputFormat(TextInputFormat.class);
 conf1.setOutputFormat(TextOutputFormat.class);

 FileInputFormat.setInputPaths(conf1, new Path(name));
 FileOutputFormat.setOutputPath(conf1, new Path(name3));

 JobClient.runJob(conf1);
 Writer writer = new BufferedWriter(new OutputStreamWriter(
         new FileOutputStream(name3+"1"), "utf-8")) ;
 
 System.out.println(freq.size());
 int i=0;
 for(String t :freq.keySet() )
 {	 i++;
 System.out.println(i);
	 if(freq.get(t)>=Integer.parseInt(num)){
writer.write(t+"	"+freq.get(t));
writer.write("\n");
	 }
	 
 }
 writer.close();
 }
 
 
 public static void generation(List<String[]> al){
 
 if(al.size()>0)
 {
	 List<String[]> li = candidatepair(al);
	 
	 
 if(li.size()>0&&li.get(1).length<3){
	 /*for(int i=0;i<li.size();i++)
	 {
		 list2.add(li.get(i));
	 }
	 */
	 list2.addAll(li);
	 generation(li);
 }
 }

 
}
 public static List<String[]>  candidatepair(List<String[]> a){
	 list3=new ArrayList<>();
	 for(int i=0;i<a.size();i++){
		 String[] first=a.get(i);
		 for(int j=i+1;j<a.size();j++){
			 String[] second=a.get(j);
			 
			 if(Arrays.equals(Arrays.copyOfRange(first, 0, first.length-1), Arrays.copyOfRange(second, 0, second.length-1))){
						 String[] result = Arrays.copyOf(first, first.length +1);
						 String element=second[second.length-1];
						 result[first.length]=element;
						 
				 list3.add(result);
			 }
		 }
		 
	 }
	
	
	return list3;
	 
 }
}



