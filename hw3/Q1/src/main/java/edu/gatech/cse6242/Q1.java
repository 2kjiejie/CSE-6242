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


public class Q1 {
/* TODO: Update variable below with your gtid */
  final String gtid = "jzhou417";
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
 //   private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    //public Double total = 0.00;
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //System.out.println(value);
      StringTokenizer itr = new StringTokenizer(value.toString(),",");	
      String pickupid = itr.nextToken();
      itr.nextToken();
	Double distance = Double.parseDouble(itr.nextToken().toString());
      //System.out.println(distance);
      if (distance != 0 ){
	String totalf = itr.nextToken().toString();
        Double totalfare = Double.parseDouble(totalf);
	if (totalfare > 0){
	word.set(pickupid);
 	Integer one = 1;
        //System.out.println(word);
        context.write(word, new Text(one+","+totalf));
	}     
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    //private TupleWritable result = new TupleWritable();
    //private DoubleWritable result1 = new DoubleWritable();
    //private DoubleWritable result2 = new DoubleWritable();
    //private Text final_result = new Text();
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      double sum = 0;
      int count =0;
    
      for (Text val : values) {
	String[] total = val.toString().split(",");
	Double tfare = Double.parseDouble(total[1]);	
        count += Integer.parseInt(total[0]);
	sum += tfare;
      }
	String totalfare1 = String.format("%,.2f",sum);
      context.write(key, new Text(count+","+totalfare1));
    }
  }	
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q1");
    /* TODO: Needs to be implemented */
    job.setJarByClass(Q1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    ////	
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
