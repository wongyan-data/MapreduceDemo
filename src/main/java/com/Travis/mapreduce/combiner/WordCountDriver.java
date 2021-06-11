package com.Travis.mapreduce.combiner;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;

import java.io.IOException;


public class WordCountDriver {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
//   1.获取job
        Configuration conf = new Configuration ();
        Job job = Job.getInstance (conf);
//   2.设置jar包路径
        job.setJarByClass(WordCountDriver.class);
//   3.关联mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
//   4.设置map输出kV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
//   5.设置最终输出的kV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //       设置虚拟切片的大小
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        job.setCombinerClass (Wordcombiner.class);
//   6. 设置输出路径和输入路径
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths (job,new Path("E:\\java files\\Project1\\input"));
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job,new Path("E:\\java files\\Project1\\C1"));
//   7.提交job
        boolean result = job.waitForCompletion (true);
        System.exit(result ? 0 : 1);
    }
}
