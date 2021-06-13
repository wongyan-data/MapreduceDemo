package com.Travis.mapreduce.MapJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text ,NullWritable> {
private Text outK = new Text ();


    private  HashMap<String, String> pdmap = new HashMap<> ();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        huo qu huan cun de wen jian  , bing ba hunacun de nei rong feng zhuang dao  pd.txt
        URI[] cacheFiles = context.getCacheFiles ();// huan  cun di zhi  bushi  yi ge liu  ,  xu yao you re qu  du zhe ge  liu


        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get (context.getConfiguration ());
        FSDataInputStream fis = fs.open (new Path (cacheFiles[0]));

//      cong liu  zhong du qu shuju
        BufferedReader reader = new BufferedReader (new InputStreamReader (fis, "UTF-8"));
        String  line ;
        while (StringUtils.isNotEmpty (line=reader.readLine ())){
//            qie ge
            String[] fidles = line.split ("\t");
            pdmap.put (fidles[0],fidles[1]);


        }
//        guanliu

        IOUtils.closeStream (reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//
        String line = value.toString ();
        String[] fields = line.split ("\t");

//        huo qu pid
        String pname = pdmap.get (fields[1]);

//        huo uq ding dan id  he  ding dan shu liang

//   feng  zhuang
        outK.set (fields[0] + "\t" + pname + "\t" + fields[2]);
        context.write (outK , NullWritable.get ());


    }
}
