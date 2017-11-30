package frequentItems;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import frequentItems.FreqItems.Map;

public class randomized {
	static int threshold=0;
	static List<String> list2 = new ArrayList<String>();
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		   
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
		    	
		    	
		    	for(int j=i+1;j<=list.size()-1;j++)
		    	{
		    		list1.add((list.get(i)+","+list.get(j)).toString());
		    	}
		    }
		    for(int i=0;i<=list.size()-1;i++)
		    {
		    	
		    	
		    	for(int j=i+1;j<=list.size()-1;j++)
		    	{	
		    		for(int k=j+1;k<=list.size()-1;k++)
		    		{
		    		list1.add((list.get(i)+","+list.get(j)+","+list.get(k)).toString());
		    	
		    		}
		    	}
		    }

		    for(int i=0;i<=list1.size()-1;i++)
		    {
		    	
		    	output.collect(new Text(list1.get(i)),new IntWritable(1));
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
		
	public static void main (String args[]) throws IOException{
		long startTime = System.currentTimeMillis();
        File folder= new File(args[0]);
        random(folder); 
        File file=new File("/home/v/v_yakkan/Desktop/Input1.txt");
        readFile(file);
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
		 FileReader fileReader = 
		         new FileReader("/home/v/v_yakkan/Desktop/output/part-00000");
		 BufferedReader bufferedReader1 = 
		         new BufferedReader(fileReader);
		String a=null;
		List<String> al = new ArrayList<>();
		 while((a=bufferedReader1.readLine())!=null){
			 String[] parts=a.split("\\t");
			 if(Integer.parseInt(parts[1])>=threshold/125){
			 list2.add(parts[0]);
			 if(StringUtils.ordinalIndexOf(parts[0], ",", 2)!=-1){
				 al.add(parts[0]);
			 }
		 }
			 
		 }
		 for(int i=0;i<list2.size()-1;i++)
		 {
			 System.out.println(list2.get(i));
		 }
	}

	private static void random(File file) throws IOException {
		FileReader fileReader = 
		         new FileReader(file);
		 BufferedReader bufferedReader = 
		         new BufferedReader(fileReader);
		int j=0;
		String line =null;
		int threshold=0;
		List<Integer> randomnum=new ArrayList<>();
		for(int i=0;i<2000;i++)
		{
			randomnum.add(i);
		}
		Collections.shuffle(randomnum);
		List<Integer> randomnum1=new ArrayList<>();
		for(int i=0;i<200;i++)
		{
			randomnum1.add(randomnum.get(i));
		}
		Collections.sort(randomnum1);
		 System.out.println("elapsedTime");
		Writer writer = new BufferedWriter(new OutputStreamWriter(
		          new FileOutputStream("/home/v/v_yakkan/Desktop/Input1.txt"), "utf-8"));
		while((line=bufferedReader.readLine())!=null){
			
			if(j==0)
			threshold=Integer.parseInt(line);
			else
			{
				if(randomnum1.contains(j)){
					 writer.write(line);
					 writer.write("\n");
				}
			}
			j++;
		}
		writer.close();
	}

}
