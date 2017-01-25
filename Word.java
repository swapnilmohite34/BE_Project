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

public class Word extends Configured implements Tool{
	public static void main (String[] args) throws Exception{
		int res = ToolRunner.run(new Word(), args);
		
		System.exit(res);
	}
	
	public int run(String[] args) throws Exception{
		Job job = Job.getInstance(getConf(), "word");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Text filename = new Text();
		private Text key = new Text();
		private Text token = new Text(); 
		private String str,str1,pstr,para; 
		private long numRecords = 0;
		String[] tokens,tokens1;
		int docid=1;
		int id,id1;
		private HashSet<String> stopWordes = new HashSet<String>();

		public void setup(Context ctx) throws IOException{
			stopWordes = new HashSet<String>();
        		stopWordes.add("I"); stopWordes.add("a");
        		stopWordes.add("about"); stopWordes.add("an");
        		stopWordes.add("are"); stopWordes.add("as");
        		stopWordes.add("at"); stopWordes.add("be");
        		stopWordes.add("by"); stopWordes.add("com");
        		stopWordes.add("de"); stopWordes.add("en");
        		stopWordes.add("for"); stopWordes.add("from");
        		stopWordes.add("how"); stopWordes.add("in");
        		stopWordes.add("is"); stopWordes.add("it");
        		stopWordes.add("la"); stopWordes.add("of");
        		stopWordes.add("on"); stopWordes.add("or");
        		stopWordes.add("that"); stopWordes.add("the");
        		stopWordes.add("this"); stopWordes.add("to");
        		stopWordes.add("was"); stopWordes.add("what");
        		stopWordes.add("when"); stopWordes.add("where");
        		stopWordes.add("who"); stopWordes.add("will");
        		stopWordes.add("with"); stopWordes.add("and");
        		stopWordes.add("the"); stopWordes.add("www");	
			stopWordes.add(".");
			stopWordes.add("!");
			stopWordes.add(",");
			stopWordes.add("then");
			stopWordes.add("?");
		}
		
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException{
			String filenameStr=((FileSplit) context.getInputSplit()).getPath().getName();
			String line = lineText.toString().toLowerCase();
			Document document = Jsoup.parse(line);   
			String title1 = document.title();   //Print title.
			Elements paragraphs = document.select("p");
 			for(Element p : paragraphs){
  			para = p.text().toString();   
			//Optimization Type 1 : Split method
				
			tokens = para.split(" ");
			for(int i=0; i<tokens.length; i++)
			{
				if(tokens[i].isEmpty())
				{
					continue;
				}
				else if(!stopWordes.contains(tokens[i]))
				token.set(""+tokens[i]+" "+filenameStr);
				context.write(token, one);
			}
			}
			tokens1 = title1.split(" ");
			for(int i=0; i<tokens1.length; i++)
			{
				if(tokens1[i].isEmpty())
				{
					continue;
				}
				else if(!stopWordes.contains(tokens1[i]))		
				token.set(""+tokens1[i]+" "+filenameStr);
				context.write(token, one);
			}
				 
		}

	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private HashSet<String> stm = new HashSet<String>();
		TreeMap<String,Integer> tm = new TreeMap<String,Integer>();
		private Text token = new Text(); 
		public void reduce(Text word, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable count : counts){
				sum += count.get();
			}
		tm.put(word.toString(),sum);		
		//context.write(word, new IntWritable(sum));
		}
		public void cleanup(Context ctx) throws IOException, InterruptedException{
			TreeMap<String,Integer> stm = SortbyValue(tm);
			Set<String> st = stm.keySet();
			int count = 0;
			Text key = new Text();
			IntWritable val = new IntWritable();
			for(String k:st)
			{	
				key.set(k);
				if(tm.get(k) != null)
				{
					val = new IntWritable(tm.get(k));
					ctx.write(key,val);
					++count;

					/*if(count == 15)
						break;*/
				}

			} 
		}

		public static TreeMap<String,Integer> SortbyValue(TreeMap<String,Integer> base)
		{
			ValueComparator vc = new ValueComparator(base);
			
			TreeMap<String,Integer> sortedTS = new TreeMap<String,Integer>(vc);
					
			sortedTS.putAll(base);	
			return sortedTS;
		}
		
		public static class ValueComparator implements Comparator<String>
		{
			TreeMap<String,Integer> ts;
			public ValueComparator(TreeMap<String,Integer> base)
			{
				this.ts = base;
			}
			public int compare(String k1, String k2)
			{	
				if(ts.get(k1) >= ts.get(k2))
				 return -1;
				else
				 return 1;
			}
		}

		

	}
}
