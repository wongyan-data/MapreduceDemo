package com.Travis.mapreduce.MapJoin;

import com.Travis.mapreduce.reduceJoin.TableBean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {


    public  static  void  main(String[]  args) throws IOException,
            ClassNotFoundException, InterruptedException, URISyntaxException {
        Job job = Job.getInstance(new Configuration ());

        job.setJarByClass(MapJoinDriver.class);
        job.setMapperClass(MapJoinMapper.class);
//
//        she zhi map shuchu de KV leixing
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

//        she zhi zuizhong shuchu de KV lexing
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        // jia zai huan chun lu jing
        job.addCacheFile(new URI("file:///D:/input1/tablecache/pd.txt"));
        // Map 端 Join 的逻辑不需要 Reduce 阶段，设置 reduceTask 数量为 0
        job.setNumReduceTasks(0);

//        she zhi  shu ru shu chu lu Jing
        FileInputFormat.setInputPaths(job, new Path ("E:\\java files\\Project1\\11_input\\inputtable2"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\java files\\Project1\\outputTable2"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
