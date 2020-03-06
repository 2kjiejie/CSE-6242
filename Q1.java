package edu.gatech.cse6242;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1 {
/* TODO: Update variable below with your gtid */
  final String gtid = "jzhou417";
  //private CountInformationTuple outCountOrder = new CountInformationTuple();

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
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
        Double totalfare = Double.parseDouble(itr.nextToken().toString());
	if (totalfare > 0){
	word.set(pickupid);
        //System.out.println(word);
        context.write(word, one);
	}     
      }
      
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
	
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
	System.out.println(val);        
	sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
/////
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q1");

    /* TODO: Needs to be implemented */
    job.setJarByClass(Q1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    ////	
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


}
