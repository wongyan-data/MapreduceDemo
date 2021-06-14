package com.Travis.mapreduce.compress;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class WordCountDriver {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
//   1.获取job
        Configuration conf = new Configuration ();
        Job job = Job.getInstance (conf);

        // 开启 map 端输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);

        // 设置 map 端输出压缩方式
        conf.setClass("mapreduce.map.output.compress.codec",
                BZip2Codec.class,CompressionCodec.class);
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
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);


        //   6. 设置输出路径和输入入境
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths (job,new Path("E:\\java files\\Project1\\input"));
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job,new Path("E:\\java files\\Project1\\compressOutput"));



        // 设置 reduce 端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);

        // 设置压缩的方式
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
//     FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//     FileOutputFormat.setOutputCompressorClass(job,DefaultCodec.class);


        //   7.提交job
        boolean result = job.waitForCompletion (true);
        System.exit(result ? 0 : 1);
    }
}
