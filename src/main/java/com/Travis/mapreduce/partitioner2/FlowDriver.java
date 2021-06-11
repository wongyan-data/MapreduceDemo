package com.Travis.mapreduce.partitioner2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //    1.获取job
        Configuration conf = new Configuration ();
        Job job =Job.getInstance(conf);

//    2.设置jar包
        job.setJarByClass (FlowDriver.class);

//    3.关联Mapper和Reduce
        job.setMapperClass (FlowMapper.class);
        job.setReducerClass (FlowReduce.class);

//    4.设置Mapper输出的key和value类型
        job.setMapOutputKeyClass (Text.class);
        job.setMapOutputValueClass (FlowBean.class);

//    5.设置最终输出的key和value类型
        job.setOutputKeyClass (Text.class);
        job.setOutputValueClass (FlowBean.class);

        //8 指定自定义分区器
        job.setPartitionerClass(ProvincePartitioner.class);
        //9 同时指定相应数量的 ReduceTask
        job.setNumReduceTasks(5);
//    6.设置数据的输入路径和输出路径
        FileInputFormat.setInputPaths (job,new Path ("E:\\java files\\Project1\\inputnumber"));
        FileOutputFormat.setOutputPath (job , new Path ("D:\\output11111"));
//    7.提交job
        boolean result = job.waitForCompletion (true);
        System.exit (result?0:1);

    }

}
