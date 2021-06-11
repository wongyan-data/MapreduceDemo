package com.Travis.mapreduce.OutputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import sun.nio.ch.IOUtil;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter {

    private   FSDataOutputStream otherout;
    private   FSDataOutputStream atguiguout;

    public LogRecordWriter(TaskAttemptContext job) {
        try {
            FileSystem fs = FileSystem.get (job.getConfiguration ());
             atguiguout = fs.create (new Path ("D:\\hadoop\\atguigu.log"));
             otherout = fs.create (new Path ("D:\\hadoop\\other.log"));
        } catch (IOException e) {
            e.printStackTrace ();
        }


    }

    @Override
    public void write(Object key, Object value) throws IOException, InterruptedException {
//  具体写入
         String log  = key.toString ();
         if (log.contains ("atguigu")){
             atguiguout.writeBytes (log+"\n");
         }else{
             otherout.writeBytes (log+"\n");
         }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
//  关闭流
        IOUtils.closeStream (atguiguout);
        IOUtils.closeStream (otherout);
    }
}
