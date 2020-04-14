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

public class Q4a {
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
private final static IntWritable mone = new IntWritable(-1);
    private Text word1 = new Text();
private Text word2 = new Text();

    //public Double total = 0.00;
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //System.out.println(value);
      StringTokenizer itr = new StringTokenizer(value.toString());	
      String pickupid = itr.nextToken();
      String doid = itr.nextToken();
      //System.out.println(distance);
        //System.out.println(word);
word1.set(pickupid);
context.write(word1, one);
word2.set(doid);
context.write(word2, mone);     
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    //private TupleWritable result = new TupleWritable();
    //private DoubleWritable result1 = new DoubleWritable();
    //private DoubleWritable result2 = new DoubleWritable();
    //private Text final_result = new Text();
	private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static class TokenizerMapper2
       extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    //public Double total = 0.00;
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //System.out.println(value);
      StringTokenizer itr = new StringTokenizer(value.toString());
      itr.nextToken();	
      String countid = itr.nextToken();
      //System.out.println(distance);
        //System.out.println(word);
	word.set(countid);
	context.write(word, one);
    }
  }


  public static void main(String[] args) throws Exception {

    /* TODO: Update variable below with your gtid */
    final String gtid = "jzhou417";

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q4a");

    /* TODO: Needs to be implemented */
job.setJarByClass(Q4a.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    Path outputPath = new Path("NEW1");
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, outputPath);
    outputPath.getFileSystem(conf).delete(outputPath);
    job.waitForCompletion(true);
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2);

    job2.setJarByClass(Q4a.class);
    job2.setMapperClass(TokenizerMapper2.class);
    job2.setCombinerClass(IntSumReducer.class);
    job2.setReducerClass(IntSumReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    Path outputPath1 = new Path(args[1]);
    FileInputFormat.addInputPath(job2,outputPath);
    FileOutputFormat.setOutputPath(job2, outputPath1);
    outputPath1.getFileSystem(conf2).delete(outputPath1);
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
