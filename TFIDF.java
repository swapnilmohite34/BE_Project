package org.myorg;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TFIDF extends Configured implements Tool{
	public static void main (String[] args) throws Exception{
		int res = ToolRunner.run(new TFIDF(), args);
		System.exit(res);
	}
	
	public int run(String[] args) throws Exception{
		Job job = Job.getInstance(getConf(), "Term frquency");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		//job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable();
		private Text word = new Text();
		int i,j,s,c=0;
		private Text filename = new Text();
		private Text key = new Text();
		private Text token = new Text(); 
		private String str,str1,pa="",para; 
		private long numRecords = 0;
		String[] tokens,tokens1;
		String query = "news about presidential campaign";
		String[] queryTokens=query.split(" ");
		//Iterator itr;
		int res;
		ArrayList<String> list=new ArrayList<String>();
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException{
			String filenameStr=((FileSplit) context.getInputSplit()).getPath().getName();
			String line = lineText.toString().toLowerCase();
			System.out.println("line"+line);
			Document document = Jsoup.parse(line);  
			Elements paragraphs = document.select("p");
			s = document.select("p").size();
 			for(Element p : paragraphs){
  				list.add(p.text());
			c++;
			Iterator itr=list.iterator();
			while(itr.hasNext()){   
  			pa=pa + itr.next().toString()+" "; 
  			}
			tokens = pa.split(" ");
			System.out.println(list);
			for(i=0;i<tokens.length;i++)
			{
				int count=0;
				for(j=0;j<queryTokens.length;j++)
				{
					if(tokens[i].equals(queryTokens[j]))
					{
						count++;
					}
				}
					res=res+count;
			}
			System.out.println(c);
			if(c==c){
			token.set(filenameStr);
			context.write(token, new IntWritable(res));
			}		

			
		}}

	}	
}
