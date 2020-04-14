package edu.gatech.cse6242;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
public class Q4b {
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
   // private final static IntWritable plus = new IntWritable(1);

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());
      itr.nextToken();
      itr.nextToken();	
      String pasct = itr.nextToken().toString();
      String tf = itr.nextToken().toString();
      Integer one = 1;

      word.set(pasct);
	context.write(word,new Text(one+","+tf));   
    }
  }
  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
  
	
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      
	double result = 0;
	double sum = 0;
	int count = 0;
      for (Text val : values) {
	String[] lines = val.toString().split(",");	
	count +=  Integer.parseInt(lines[0]);
        sum += Double.parseDouble(lines[1]);
	
      }

	result =(sum/count);
	String final_re = String.format("%.2f",result);
	
      context.write(key, new Text(final_re));
    }
  }
 
 

  public static void main(String[] args) throws Exception {
    
    final String gtid = "jzhou417";
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "job");

    job.setJarByClass(Q4b.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
