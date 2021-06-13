package com.Travis.mapreduce.etl;


import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WebLogMapper extends Mapper<LongWritable ,Text ,Text , NullWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//  183.49.46.228 - - [18/Sep/2013:06:49:23 +0000] "-" 400 0 "-" "-"
//        1.huo  qu  yi hang
        String line = value.toString ();

//        2.qie  ge

//        3.ETL
        boolean result  = parseLog(line, context);
        
        if(!result){
            return;
        }
//        4.xie chu
        context.write (value , NullWritable.get ());
    }


//    feng  zhuang ri zhi de fang fa
    private boolean parseLog(String line, Context context) {

//     1. qie ge
//        183.49.46.228 - - [18/Sep/2013:06:49:23 +0000] "-" 400 0 "-" "-"
        String[] fields = line.split (" ");
//     2. pan duan ri zhi de chang du shi bu shi da yu 11
        if (fields.length  > 11 ){
            return true ;
        } else {
            return false ;
        }

    }
}
